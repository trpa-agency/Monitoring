"""
TRPA Goshawk Threshold Zone — v1 promoted runner.

For each nest in `nogo_nest_usfs`, builds a single polygon consisting of a
0.25-mile buffer around the nest plus closest contiguous HIGH-then-MOD
CWHR habitat polygons until projected post-close TotalAc >= 500 ac.
Output is one row per nest in `TRPA_Goshawk_Threshold_Zone` in the
dedicated output GDB at `C:\\GIS\\Goshawk_Threshold_Zone.gdb`.

Algorithm summary (the simpler v1 — see methods.md for the full picture
and the changelog of what was tried and discarded):

    1. Buffer 0.25 mi around the nest (geodesic).
    2. Pre-clip HIGH and MOD habitat to a 5-mile envelope around the nest.
    3. Erase the buffer footprint from each pre-clipped layer.
    4. Add Acres (geodesic) and NEAR_DIST (to the nest point) to each.
    5. Tiered growth, INTERSECT contiguity, NEAR_DIST ascending order,
       ties broken by larger Acres:
       a. Drain HIGH closest to the nest until projected post-close
          TotalAc >= target or no contiguous HIGH remains.
       b. If still short, take ONE MOD pick, then return to draining
          HIGH (the new footprint may now touch HIGH that didn't
          reach before).
       c. Stop when projected total >= target or no contiguous HIGH
          or MOD remains.
    6. Finalize: Dissolve -> fill interior holes (CONTAINED_ONLY) ->
       morphological close (Buffer +CLOSE_DIST then Buffer -CLOSE_DIST)
       -> re-fill interior holes.
    7. Write one feature with NestID, NestOID, Location, BufAc, HighAc,
       ModAc, TotalAc, Picks, Sec, Short.

Stop condition is post-close projected TotalAc, evaluated by a periodic
probe that runs the full final pipeline on a copy of the in-progress
footprint. Without this, the close adds 30-280 ac of "Filled" on top
of the pre-fill running sum and the final TotalAc systematically
overshoots.

After the build loop, a postprocess trim runs (controlled by `DO_TRIM`):
    8. Identity overlay of the threshold-zone FC vs. the source veg
       layer -> per-zone pieces with `PieceAc` (geodesic).
    9. For each over-target zone, sort its pieces by distance to the
       parent nest, cumulative-sum `PieceAc`, drop everything past the
       cutoff, dissolve the kept pieces. Buffer pieces are at distance
       0 so they always survive — every output zone always contains
       its nest.
   10. Write the trimmed FC `TRPA_Goshawk_Threshold_Zone_Trimmed` with
       the original schema plus `OrigAc` (pre-trim total) and
       `Trimmed` (1/0).

Run:
    "C:\\Program Files\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3\\python.exe" goshawk_threshold_zone.py
"""
from __future__ import annotations

import logging
import os
import time
import traceback
import uuid
from pathlib import Path

import arcpy

arcpy.env.overwriteOutput = True


# -------------------
# LOGGING
# -------------------
LOGGER_NAME = "goshawk_threshold_zone"
log = logging.getLogger(LOGGER_NAME)


def _setup_logging(level: int = logging.INFO) -> Path:
    """Configure console + timestamped file logging. Returns the log file path."""
    logs_dir = Path(__file__).resolve().parent / "logs"
    logs_dir.mkdir(exist_ok=True)

    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    log_file = logs_dir / f"{LOGGER_NAME}_{timestamp}.log"

    log.setLevel(level)
    log.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    log.addHandler(console)

    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    log.info(f"Log file: {log_file}")
    return log_file


# -------------------
# HELPERS
# -------------------
def mem(prefix: str) -> str:
    """Generate a unique in_memory feature class path."""
    return fr"in_memory\{prefix}_{uuid.uuid4().hex[:8]}"


def clear_in_memory() -> None:
    """Wipe everything in the in_memory workspace. Safe to call any time."""
    try:
        arcpy.management.Delete("in_memory")
    except Exception as e:
        log.warning(f"clear_in_memory: {type(e).__name__}: {e}")


def ensure_file_gdb(gdb_path: str) -> None:
    """Create gdb_path if it doesn't exist."""
    if arcpy.Exists(gdb_path):
        return
    out_dir = os.path.dirname(gdb_path)
    out_name = os.path.basename(gdb_path)
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    arcpy.management.CreateFileGDB(out_dir, out_name)
    log.info(f"Created output GDB: {gdb_path}")


def add_acres(
    fc: str, field_name: str = "Acres", method: str = "AREA_GEODESIC"
) -> None:
    """Add Acres field if missing; (re)compute area in acres."""
    fields = {f.name for f in arcpy.ListFields(fc)}
    if field_name not in fields:
        arcpy.management.AddField(fc, field_name, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(
        fc, [[field_name, method]], area_unit="ACRES"
    )


def sum_field(fc: str, field_name: str) -> float:
    total = 0.0
    with arcpy.da.SearchCursor(fc, [field_name]) as cur:
        for (v,) in cur:
            total += float(v or 0.0)
    return total


def fill_interior_holes(
    poly_fc: str, out_path: str, hole_area_threshold: float = 10**18
) -> str:
    """
    Eliminate INTERIOR holes only. Multipart exterior parts are
    preserved (so the buffer is never silently dropped).

    `part_option="CONTAINED_ONLY"` is the default but is set explicitly
    here because the earlier version of this pipeline used `"ANY"`,
    which silently deletes every multipart polygon part except the
    largest one — it's the bug that caused several nests to end up not
    covered by their threshold zone.
    """
    arcpy.management.EliminatePolygonPart(
        in_features=poly_fc,
        out_feature_class=out_path,
        condition="AREA",
        part_area=hole_area_threshold,
        part_option="CONTAINED_ONLY",
    )
    return out_path


# -------------------
# CORE PER-NEST PROCESS
# -------------------
def build_zone_for_nest(
    nest_oid: int,
    nest_id: str,
    location: str,
    *,
    nests_fc: str,
    hab_high: str,
    hab_mod: str,
    target_ac: float = 500.0,
    buffer_dist: str = "0.25 Miles",
    search_radius: str = "5 Miles",
    min_acres: float = 0.5,
    close_dist: str = "200 Feet",
    probe_every: int = 5,
    probe_homestretch_ac: float = 100.0,
    max_iters: int = 5000,
):
    """
    Run the v1 pipeline for one nest.

    Returns:
        (final_geom, buf_ac, high_added, mod_added, total_ac, n_picks, dt)
    """
    t0 = time.time()

    # ---- Buffer ----
    nest_lyr = mem("nest")
    arcpy.management.MakeFeatureLayer(
        nests_fc, nest_lyr, f"OBJECTID = {nest_oid}"
    )
    buf_fc = mem("buf")
    arcpy.analysis.Buffer(nest_lyr, buf_fc, buffer_dist, dissolve_option="ALL")
    add_acres(buf_fc)
    buf_ac = sum_field(buf_fc, "Acres")

    # ---- Pre-clip HIGH and MOD to the search envelope ----
    search_buf = mem("search_buf")
    arcpy.analysis.Buffer(nest_lyr, search_buf, search_radius, dissolve_option="ALL")

    def _local_subset(src_fc: str, label: str) -> str:
        pre_lyr = mem(f"{label}_pre")
        arcpy.management.MakeFeatureLayer(src_fc, pre_lyr)
        arcpy.management.SelectLayerByLocation(pre_lyr, "INTERSECT", search_buf)
        local_fc = mem(f"{label}_local")
        arcpy.management.CopyFeatures(pre_lyr, local_fc)
        arcpy.management.Delete(pre_lyr)
        return local_fc

    high_local = _local_subset(hab_high, "high")
    mod_local = _local_subset(hab_mod, "mod")

    # ---- Erase buffer footprint from each ----
    high_work = mem("high_work")
    mod_work = mem("mod_work")
    arcpy.analysis.Erase(high_local, buf_fc, high_work)
    arcpy.analysis.Erase(mod_local, buf_fc, mod_work)

    # ---- Acres + Near ----
    add_acres(high_work)
    add_acres(mod_work)
    arcpy.analysis.Near(high_work, nest_lyr)
    arcpy.analysis.Near(mod_work, nest_lyr)

    # ---- Growth setup ----
    high_lyr = mem("high_lyr")
    mod_lyr = mem("mod_lyr")
    arcpy.management.MakeFeatureLayer(high_work, high_lyr)
    arcpy.management.MakeFeatureLayer(mod_work, mod_lyr)
    oid_high = arcpy.Describe(high_work).OIDFieldName
    oid_mod = arcpy.Describe(mod_work).OIDFieldName

    current_fc = mem("current")
    arcpy.management.CopyFeatures(buf_fc, current_fc)

    selected_high: set = set()
    selected_mod: set = set()
    high_added = 0.0
    mod_added = 0.0
    projected_total = buf_ac
    picks_since_probe = 0

    def pick_best_contiguous(layer, oid_field, selected):
        arcpy.management.SelectLayerByLocation(
            layer, "INTERSECT", current_fc, selection_type="NEW_SELECTION"
        )
        best = None
        with arcpy.da.SearchCursor(
            layer, [oid_field, "Acres", "NEAR_DIST"]
        ) as cur:
            for oid, ac, nd in cur:
                if oid in selected:
                    continue
                ac = float(ac or 0.0)
                if ac < min_acres:
                    continue
                nd = float(nd if nd is not None else 1e18)
                cand = (nd, -ac, oid)
                if best is None or cand < best:
                    best = cand
        if best is None:
            return None
        return best[2], -best[1]

    def append_to_footprint(work_fc, oid, oid_field):
        with arcpy.da.SearchCursor(
            work_fc, ["SHAPE@"], where_clause=f"{oid_field} = {oid}"
        ) as cur:
            for (g,) in cur:
                with arcpy.da.InsertCursor(current_fc, ["SHAPE@"]) as ic:
                    ic.insertRow([g])
                return

    def project_total_ac() -> float:
        """Run the full finalize pipeline on a copy of current_fc and
        return geodesic TotalAc. Cleans up its temps before return."""
        diss_p = mem("probe_diss")
        arcpy.management.Dissolve(current_fc, diss_p)
        pre_close_p = mem("probe_pre_close")
        fill_interior_holes(diss_p, pre_close_p)
        close_out_p = mem("probe_close_out")
        arcpy.analysis.Buffer(pre_close_p, close_out_p, close_dist)
        closed_p = mem("probe_closed")
        arcpy.analysis.Buffer(close_out_p, closed_p, f"-{close_dist}")
        final_p = mem("probe_final")
        fill_interior_holes(closed_p, final_p)
        add_acres(final_p)
        total = sum_field(final_p, "Acres")
        for t in (diss_p, pre_close_p, close_out_p, closed_p, final_p):
            if arcpy.Exists(t):
                arcpy.management.Delete(t)
        return total

    n_picks = 0

    # Phase 1: drain HIGH
    for _ in range(max_iters):
        if projected_total >= target_ac:
            break
        pick = pick_best_contiguous(high_lyr, oid_high, selected_high)
        if pick is None:
            break
        oid, ac = pick
        selected_high.add(oid)
        high_added += ac
        n_picks += 1
        picks_since_probe += 1
        append_to_footprint(high_work, oid, oid_high)
        pre_fill = buf_ac + high_added + mod_added
        in_homestretch = pre_fill >= (target_ac - probe_homestretch_ac)
        if in_homestretch or picks_since_probe >= probe_every:
            projected_total = project_total_ac()
            picks_since_probe = 0

    # Phase 2: tiered HIGH -> ONE MOD -> back to HIGH
    for _ in range(max_iters):
        if projected_total >= target_ac:
            break
        pick = pick_best_contiguous(high_lyr, oid_high, selected_high)
        if pick is not None:
            oid, ac = pick
            selected_high.add(oid)
            high_added += ac
            n_picks += 1
            picks_since_probe += 1
            append_to_footprint(high_work, oid, oid_high)
        else:
            pick = pick_best_contiguous(mod_lyr, oid_mod, selected_mod)
            if pick is None:
                break
            oid, ac = pick
            selected_mod.add(oid)
            mod_added += ac
            n_picks += 1
            picks_since_probe += 1
            append_to_footprint(mod_work, oid, oid_mod)
        pre_fill = buf_ac + high_added + mod_added
        in_homestretch = pre_fill >= (target_ac - probe_homestretch_ac)
        if in_homestretch or picks_since_probe >= probe_every:
            projected_total = project_total_ac()
            picks_since_probe = 0

    # ---- Finalize ----
    dissolved = mem("dissolved")
    arcpy.management.Dissolve(current_fc, dissolved)
    pre_close = mem("pre_close")
    fill_interior_holes(dissolved, pre_close)
    close_out = mem("close_out")
    arcpy.analysis.Buffer(pre_close, close_out, close_dist)
    closed = mem("closed")
    arcpy.analysis.Buffer(close_out, closed, f"-{close_dist}")
    final_fc = mem("final")
    fill_interior_holes(closed, final_fc)
    add_acres(final_fc)
    total_ac = sum_field(final_fc, "Acres")

    with arcpy.da.SearchCursor(final_fc, ["SHAPE@"]) as cur:
        final_geom = next(cur)[0]

    dt = time.time() - t0
    return final_geom, buf_ac, high_added, mod_added, total_ac, n_picks, dt


# -------------------
# POSTPROCESS: TRIM TO TARGET
# -------------------
def trim_zones_to_target(
    *,
    threshold_fc: str,
    output_gdb: str,
    veg_src: str,
    nests_fc: str,
    target_ac: float,
    id1_name: str = "TRPA_GTZ_x_Veg",
    trim_name: str = "TRPA_Goshawk_Threshold_Zone_Trimmed",
):
    """
    Postprocess: any zone in `threshold_fc` whose `TotalAc > target_ac`
    is dissolved back to ≤ `target_ac` along real veg-piece boundaries.

    Steps:
      1. `arcpy.analysis.Identity` of threshold zones × `veg_src` ->
         `id1_name` (kept in output_gdb so it can be inspected).
      2. Add per-piece geodesic `PieceAc` field on the Identity result.
      3. For each piece, compute geodesic distance to its parent
         nest's point geometry (looked up by `NestOID`). Buffer pieces
         are at distance 0; outermost close-fill bits are far.
      4. Group pieces by `NestID`, sort each group by distance asc.
         Cumulative-sum `PieceAc`; stop before the next piece would
         push past `target_ac`. Drop everything from there outward.
      5. Dissolve kept pieces per nest into a new trimmed polygon,
         recompute geodesic acres, write to `trim_name` with original
         schema plus `OrigAc` (pre-trim total) and `Trimmed` (1/0).

    Buffer pieces are always at distance 0, so they're always in the
    kept set — every output zone always contains its nest.

    Returns (trim_fc_path, n_trimmed, id1_fc_path).
    """
    log.info("-" * 60)
    log.info("Postprocess: trim over-target zones to <= %.1f ac" % target_ac)
    log.info("-" * 60)
    t_start = time.time()

    # 1. Identity threshold zones x veg
    id1_fc = output_gdb + "\\" + id1_name
    if arcpy.Exists(id1_fc):
        arcpy.management.Delete(id1_fc)
    log.info(f"Running Identity: {threshold_fc} x {veg_src}")
    arcpy.analysis.Identity(
        in_features=threshold_fc,
        identity_features=veg_src,
        out_feature_class=id1_fc,
        join_attributes="ALL",
    )
    n_pieces = int(arcpy.management.GetCount(id1_fc)[0])
    log.info(f"  Identity result: {n_pieces:,} pieces -> {id1_fc}")

    # 2. PieceAc on the result
    add_acres(id1_fc, "PieceAc", "AREA_GEODESIC")
    log.info(f"  PieceAc computed (geodesic).")

    # 3. Per-piece distance to the parent nest's point geometry
    nest_geoms: dict = {}
    with arcpy.da.SearchCursor(nests_fc, ["OBJECTID", "SHAPE@"]) as cur:
        for oid, g in cur:
            nest_geoms[oid] = g

    oid_field_id1 = arcpy.Describe(id1_fc).OIDFieldName
    pieces_by_nest: dict = {}  # nest_id -> [(piece_oid, piece_ac, dist), ...]
    with arcpy.da.SearchCursor(
        id1_fc,
        [oid_field_id1, "NestID", "NestOID", "PieceAc", "SHAPE@"],
    ) as cur:
        for poid, nid, noid, pa, geom in cur:
            ng = nest_geoms.get(noid)
            if ng is None or geom is None:
                d = float("inf")
            else:
                d = geom.distanceTo(ng)
            pieces_by_nest.setdefault(nid, []).append((poid, float(pa or 0.0), d))
    for nid in pieces_by_nest:
        pieces_by_nest[nid].sort(key=lambda r: r[2])

    # 4. Read THRESHOLD_FC; identify over-target nests and decide kept pieces
    all_threshold_rows: dict = {}
    over_target = []  # (nest_id, total_ac)
    with arcpy.da.SearchCursor(
        threshold_fc,
        ["NestID", "NestOID", "Location", "BufAc", "HighAc", "ModAc",
         "TotalAc", "Picks", "Sec", "SHAPE@"],
    ) as cur:
        for nid, noid, loc, ba, ha, ma, ta, picks, sec, geom in cur:
            all_threshold_rows[nid] = (noid, loc, ba, ha, ma, ta, picks, sec, geom)
            if ta is not None and ta > target_ac + 0.01:
                over_target.append((nid, ta))

    log.info(f"Threshold zones total: {len(all_threshold_rows)}")
    log.info(f"Over target ({target_ac:.0f} ac): {len(over_target)}")

    keep_oids_per_nest: dict = {}
    for nid, ta in over_target:
        pieces = pieces_by_nest.get(nid, [])
        keep = []
        running = 0.0
        for poid, pa, d in pieces:
            if running + pa > target_ac:
                break
            keep.append(poid)
            running += pa
        keep_oids_per_nest[nid] = (keep, running)

    # 5. Build the trimmed output FC (same schema as threshold + OrigAc, Trimmed)
    trim_fc = output_gdb + "\\" + trim_name
    if arcpy.Exists(trim_fc):
        arcpy.management.Delete(trim_fc)

    sr = arcpy.Describe(threshold_fc).spatialReference
    arcpy.management.CreateFeatureclass(
        output_gdb, trim_name, "POLYGON", spatial_reference=sr,
    )
    for fn, ft, kw in [
        ("NestID", "TEXT", {"field_length": 64}),
        ("NestOID", "LONG", {}),
        ("Location", "TEXT", {"field_length": 128}),
        ("BufAc", "DOUBLE", {}),
        ("HighAc", "DOUBLE", {}),
        ("ModAc", "DOUBLE", {}),
        ("TotalAc", "DOUBLE", {}),
        ("OrigAc", "DOUBLE", {}),
        ("Picks", "LONG", {}),
        ("Sec", "DOUBLE", {}),
        ("Trimmed", "SHORT", {}),
        ("Short", "SHORT", {}),
    ]:
        arcpy.management.AddField(trim_fc, fn, ft, **kw)

    ic_fields = [
        "SHAPE@", "NestID", "NestOID", "Location",
        "BufAc", "HighAc", "ModAc", "TotalAc", "OrigAc",
        "Picks", "Sec", "Trimmed", "Short",
    ]

    n_trimmed = 0
    with arcpy.da.InsertCursor(trim_fc, ic_fields) as ic:
        for nid, (noid, loc, ba, ha, ma, orig_ta, picks, sec, geom) in all_threshold_rows.items():
            if nid in keep_oids_per_nest:
                keep_oids, _running = keep_oids_per_nest[nid]
                if not keep_oids:
                    new_geom = geom
                    new_ta = orig_ta
                    trimmed_flag = 0
                    log.warning(f"  {nid}: no pieces to keep - using original geom")
                else:
                    where = f"{oid_field_id1} IN ({','.join(str(o) for o in keep_oids)})"
                    sel_lyr = mem("trim_sel")
                    arcpy.management.MakeFeatureLayer(id1_fc, sel_lyr, where)
                    diss = mem("trim_diss")
                    arcpy.management.Dissolve(sel_lyr, diss)
                    arcpy.management.Delete(sel_lyr)
                    with arcpy.da.SearchCursor(diss, ["SHAPE@"]) as dc:
                        new_geom = next(dc)[0]
                    add_acres(diss, "Acres", "AREA_GEODESIC")
                    new_ta = sum_field(diss, "Acres")
                    arcpy.management.Delete(diss)
                    trimmed_flag = 1
                    n_trimmed += 1
            else:
                new_geom = geom
                new_ta = orig_ta
                trimmed_flag = 0

            short = 1 if new_ta is not None and new_ta < target_ac - 0.01 else 0
            ic.insertRow([
                new_geom, nid, noid, loc,
                float(ba or 0), float(ha or 0), float(ma or 0),
                float(new_ta or 0), float(orig_ta or 0),
                int(picks or 0), float(sec or 0),
                trimmed_flag, short,
            ])

    elapsed = time.time() - t_start
    log.info(
        f"Trim postprocess done in {elapsed:.1f}s. "
        f"Trimmed: {n_trimmed} | Output: {trim_fc}"
    )
    return trim_fc, n_trimmed, id1_fc


# -------------------
# MAIN
# -------------------
def main() -> None:
    _setup_logging()
    clear_in_memory()

    # -------- USER INPUTS --------
    SOURCE_GDB = r"C:\GIS\Scratch.gdb"
    OUTPUT_GDB = r"C:\GIS\Goshawk_Threshold_Zone.gdb"

    NESTS_FC = SOURCE_GDB + "\\" + "nogo_nest_usfs"
    HAB_HIGH = SOURCE_GDB + "\\" + "amgo_ecobject_cwhr_habmodel_high"
    HAB_MOD = SOURCE_GDB + "\\" + "amgo_ecobject_cwhr_habmodel_moderate"

    # Source veg layer for the trim postprocess (Identity overlay).
    VEG_SRC = r"C:\GIS\TahoeMaps\Tahoe_Data.gdb\Vegetation_Ecobject_2010"

    OUT_NAME = "TRPA_Goshawk_Threshold_Zone"
    out_fc = OUTPUT_GDB + "\\" + OUT_NAME

    TARGET_AC = 500.0
    BUFFER_DIST = "0.25 Miles"
    SEARCH_RADIUS = "5 Miles"
    MIN_ACRES = 0.5
    CLOSE_DIST = "200 Feet"
    PROBE_EVERY = 5
    PROBE_HOMESTRETCH_AC = 100.0
    MAX_ITERS = 5000

    # Trim postprocess: True -> after the build loop, run Identity vs. veg
    # and trim any over-target zone back to <= TARGET_AC. Produces an
    # additional `_Trimmed` FC. False -> skip; only the raw FC is written.
    DO_TRIM = True

    arcpy.env.workspace = SOURCE_GDB
    ensure_file_gdb(OUTPUT_GDB)

    log.info("=" * 60)
    log.info("TRPA Goshawk Threshold Zone (v1)")
    log.info("=" * 60)
    log.info(f"Source GDB:   {SOURCE_GDB}")
    log.info(f"Output GDB:   {OUTPUT_GDB}")
    log.info(f"Nests FC:     {NESTS_FC}")
    log.info(f"HIGH habitat: {HAB_HIGH}")
    log.info(f"MOD habitat:  {HAB_MOD}")
    log.info(f"Veg source:   {VEG_SRC}")
    log.info(f"Output FC:    {out_fc}")
    log.info(f"Trim:         {DO_TRIM}")
    log.info(
        f"Target ac: {TARGET_AC} | Buffer: {BUFFER_DIST} | search: {SEARCH_RADIUS} "
        f"| min_acres: {MIN_ACRES} | close: {CLOSE_DIST}"
    )
    log.info(
        f"Probe: every {PROBE_EVERY} picks, homestretch within {PROBE_HOMESTRETCH_AC} ac"
    )

    # -------- (Re)create output FC --------
    if arcpy.Exists(out_fc):
        log.info(f"Replacing existing {out_fc}")
        arcpy.management.Delete(out_fc)

    sr = arcpy.Describe(HAB_HIGH).spatialReference
    arcpy.management.CreateFeatureclass(
        OUTPUT_GDB, OUT_NAME, "POLYGON", spatial_reference=sr
    )
    for fn, ft, kw in [
        ("NestID", "TEXT", {"field_length": 64}),
        ("NestOID", "LONG", {}),
        ("Location", "TEXT", {"field_length": 128}),
        ("BufAc", "DOUBLE", {}),
        ("HighAc", "DOUBLE", {}),
        ("ModAc", "DOUBLE", {}),
        ("TotalAc", "DOUBLE", {}),
        ("Picks", "LONG", {}),
        ("Sec", "DOUBLE", {}),
        ("Short", "SHORT", {}),
    ]:
        arcpy.management.AddField(out_fc, fn, ft, **kw)

    # -------- Pull all nest records --------
    all_nests: list[tuple[int, str, str]] = []
    with arcpy.da.SearchCursor(
        NESTS_FC, ["OBJECTID", "NEST_ID", "LOCATION"]
    ) as cur:
        for oid, nid, loc in cur:
            all_nests.append((oid, nid, loc))
    log.info(f"Nests to process: {len(all_nests)}")

    # -------- Process loop --------
    t_start = time.time()
    failures = 0
    shorts: list[str] = []

    ic_fields = [
        "SHAPE@", "NestID", "NestOID", "Location",
        "BufAc", "HighAc", "ModAc", "TotalAc",
        "Picks", "Sec", "Short",
    ]

    with arcpy.da.InsertCursor(out_fc, ic_fields) as ic:
        for i, (oid, nid, loc) in enumerate(all_nests, start=1):
            try:
                geom, buf_ac, high_a, mod_a, total_ac, n_picks, dt = (
                    build_zone_for_nest(
                        oid, nid, loc,
                        nests_fc=NESTS_FC,
                        hab_high=HAB_HIGH,
                        hab_mod=HAB_MOD,
                        target_ac=TARGET_AC,
                        buffer_dist=BUFFER_DIST,
                        search_radius=SEARCH_RADIUS,
                        min_acres=MIN_ACRES,
                        close_dist=CLOSE_DIST,
                        probe_every=PROBE_EVERY,
                        probe_homestretch_ac=PROBE_HOMESTRETCH_AC,
                        max_iters=MAX_ITERS,
                    )
                )
                short = 1 if total_ac < TARGET_AC - 0.01 else 0
                if short:
                    shorts.append(nid)
                ic.insertRow([
                    geom, nid, oid, loc,
                    float(buf_ac), float(high_a), float(mod_a),
                    float(total_ac), int(n_picks), float(dt), short,
                ])
                warn = "  WARNING short" if short else ""
                log.info(
                    f"[{i:3d}/{len(all_nests)}] {nid:>6s} "
                    f"{(loc or '')[:30]:30s} "
                    f"Total={total_ac:6.1f}  picks={n_picks:3d}  {dt:5.1f}s{warn}"
                )
            except Exception:
                failures += 1
                log.error(
                    f"[{i:3d}/{len(all_nests)}] {nid} ({loc!r}) - ERROR"
                )
                log.error(traceback.format_exc())
            finally:
                # Clean up per-nest in_memory temps so they don't pile up over 136 iterations
                clear_in_memory()

    elapsed = time.time() - t_start
    log.info("=" * 60)
    log.info(f"Build done. Output: {out_fc}")
    log.info(
        f"Processed: {len(all_nests)} | failures: {failures} | "
        f"shorts: {len(shorts)} | elapsed: {elapsed/60:.1f} min"
    )
    if shorts:
        log.info(f"Short NEST_IDs: {shorts}")

    # -------- Trim postprocess --------
    if DO_TRIM:
        try:
            trim_fc, n_trimmed, id1_fc = trim_zones_to_target(
                threshold_fc=out_fc,
                output_gdb=OUTPUT_GDB,
                veg_src=VEG_SRC,
                nests_fc=NESTS_FC,
                target_ac=TARGET_AC,
            )
            log.info("=" * 60)
            log.info(f"Trim done. Output: {trim_fc}")
            log.info(f"Trimmed: {n_trimmed} | Identity FC: {id1_fc}")
        except Exception:
            log.error("Trim postprocess failed:")
            log.error(traceback.format_exc())
    else:
        log.info("DO_TRIM=False — skipping trim postprocess.")

    clear_in_memory()
    log.info("in_memory cleared")


if __name__ == "__main__":
    main()
