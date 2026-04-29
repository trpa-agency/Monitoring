"""
TRPA Goshawk Threshold Zone — promoted runner.

For each nest in `nogo_nest_usfs`, builds a single polygon consisting of a
0.25-mile buffer plus enough contiguous HIGH-then-MOD CWHR habitat to reach
500 total acres, with all interior holes filled. Writes one row per nest to
`TRPA_Goshawk_Threshold_Zone` in `C:\\GIS\\Scratch.gdb`.

See methods.md for the full algorithm description, assumptions, and caveats.

Run:
    "C:\\Program Files\\ArcGIS\\Pro\\bin\\Python\\envs\\arcgispro-py3\\python.exe" goshawk_threshold_zone.py
"""
from __future__ import annotations

import logging
import os
import time
import traceback
import uuid
from contextlib import contextmanager
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
# TEMP / CLEANUP
# -------------------
def mem(prefix: str) -> str:
    return fr"in_memory\{prefix}_{uuid.uuid4().hex[:8]}"


def clear_in_memory() -> None:
    """
    Wipe ALL contents of the in_memory workspace. Safe to call at any
    point. Use as a belt-and-suspenders cleanup at start and end of a
    run; per-nest temps are tracked and deleted individually by
    arcpy_temp_manager.
    """
    try:
        arcpy.management.Delete("in_memory")
    except Exception as e:
        log.warning(f"clear_in_memory: {type(e).__name__}: {e}")


def ensure_file_gdb(gdb_path: str) -> None:
    """Create a file geodatabase at gdb_path if it does not already exist."""
    if arcpy.Exists(gdb_path):
        return
    out_dir = os.path.dirname(gdb_path)
    out_name = os.path.basename(gdb_path)
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    arcpy.management.CreateFileGDB(out_dir, out_name)
    log.info(f"Created output GDB: {gdb_path}")


@contextmanager
def arcpy_temp_manager():
    """Tracks in_memory temps + layers and deletes them no matter what."""
    temps: list[str] = []
    layers: list[str] = []
    try:
        yield temps, layers
    finally:
        for t in reversed(temps):
            try:
                if t and arcpy.Exists(t):
                    arcpy.management.Delete(t)
            except Exception as e:
                log.warning(f"cleanup of {t} failed: {type(e).__name__}: {e}")

        for lyr in layers:
            try:
                arcpy.management.Delete(lyr)
            except Exception as e:
                log.warning(f"cleanup of layer {lyr} failed: {type(e).__name__}: {e}")

        try:
            arcpy.ClearWorkspaceCache_management()
        except Exception as e:
            log.warning(f"ClearWorkspaceCache failed: {type(e).__name__}: {e}")


# -------------------
# HELPERS
# -------------------
def assert_exists(path: str, label: str) -> None:
    if not arcpy.Exists(path):
        raise RuntimeError(f"{label} was not created: {path}")


def add_acres(fc: str, field_name: str = "Acres", method: str = "AREA_GEODESIC") -> None:
    """
    Adds Acres field if missing and calculates area in acres.

    method="AREA_GEODESIC" (default): true geodesic acres; defensible for
        reported output numbers. Slow on layers with many polygons.
    method="AREA": planar acres. Much faster. Within ~0.5% of geodesic at
        Tahoe latitudes - acceptable for the work layers where we only
        need the value for sliver filtering and a running total.
    """
    fields = {f.name for f in arcpy.ListFields(fc)}
    if field_name not in fields:
        arcpy.management.AddField(fc, field_name, "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(
        fc, [[field_name, method]],
        area_unit="ACRES",
    )


def sum_field(fc: str, field_name: str) -> float:
    total = 0.0
    with arcpy.da.SearchCursor(fc, [field_name]) as cur:
        for (v,) in cur:
            total += float(v or 0.0)
    return total


def chunked_in_clause(field_name: str, values, chunk_size: int = 900):
    """Avoids max SQL IN() limits (FGDB is usually ~999)."""
    values = list(values)
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        yield f"{field_name} IN ({','.join(map(str, chunk))})"


def chunked_not_in_where(field_name: str, values, chunk_size: int = 900):
    """
    Build a single chunked 'field NOT IN (...)' WHERE clause, ANDing
    sub-chunks together. Returns None if `values` is empty (no filter).
    FGDB IN/NOT IN limit is ~999 per clause, so we chunk.
    """
    vals = sorted(set(values))
    if not vals:
        return None
    parts = []
    for i in range(0, len(vals), chunk_size):
        chunk = vals[i:i + chunk_size]
        parts.append(f"{field_name} NOT IN ({','.join(map(str, chunk))})")
    return " AND ".join(parts)


def copy_selected_oids(work_fc: str, oids_set, out_path: str):
    """Copy a set of OIDs from work_fc to out_path. Returns out_path or None."""
    if not oids_set:
        return None

    oid_f = arcpy.Describe(work_fc).OIDFieldName
    lyr = mem("sel_lyr")
    arcpy.management.MakeFeatureLayer(work_fc, lyr)
    arcpy.management.SelectLayerByAttribute(lyr, "CLEAR_SELECTION")

    oids = sorted(oids_set)
    for clause in chunked_in_clause(oid_f, oids):
        arcpy.management.SelectLayerByAttribute(lyr, "ADD_TO_SELECTION", clause)

    arcpy.management.CopyFeatures(lyr, out_path)
    assert_exists(out_path, "Selection copy")
    arcpy.management.Delete(lyr)
    return out_path


def fill_holes(poly_fc: str, out_path: str, hole_area_threshold: float = 10**18) -> str:
    """Removes ALL interior holes/parts by using a huge threshold."""
    arcpy.management.EliminatePolygonPart(
        in_features=poly_fc,
        out_feature_class=out_path,
        condition="AREA",
        part_area=hole_area_threshold,
        part_option="ANY",
    )
    assert_exists(out_path, "Hole-filled polygon")
    return out_path


# -------------------
# CORE PER-NEST PROCESS
# -------------------
def build_threshold_zone_for_nest(
    nests_fc: str,
    nest_oid: int,
    hab_high: str,
    hab_mod: str,
    veg_src: str,
    buffer_dist: str,
    target_total_acres: float,
    hole_area_threshold: float = 10**18,
    max_iters: int = 5000,
    min_acres: float = 2.0,
    fallback_min_acres: float = 5.0,
    search_radius: str = "5 Miles",
    close_dist: str = "200 Feet",
    probe_every: int = 5,
    probe_homestretch_ac: float = 50.0,
    contiguity_relation: str = "INTERSECT",
    fallback_search_distance: str = "0.5 Miles",
    stagnation_window: int = 5,
    stagnation_tol: float = 0.5,
    stagnation_interval: int = 10,
):
    """
    Returns (final_fc, buf_ac, high_added, mod_added, low_added, total_ac, dt).
    final_fc is an in_memory feature class (caller must delete).

    The LOW tier is derived per-nest from `veg_src` (the source vegetation
    ecobject layer): LOW = veg-within-search-radius minus HIGH minus MOD.
    There is no precomputed LOW feature class — `veg_src` is the basin-
    wide source layer (~700K polys), so it must always be pre-clipped
    to the per-nest search envelope before any other operation.

    Growth strategy (tiered preference, near-distance ordering):

      1) Drain HIGH: pick the contiguous HIGH polygon nearest to the nest
         (NEAR_DIST asc, ties broken by larger Acres). Repeat until no
         HIGH touches the footprint.
      2) If still short, add ONE contiguous MOD polygon (same ordering),
         then return to step 1 - the new footprint may now touch HIGH
         that didn't reach before.
      3) If still short and no MOD touches either, add ONE contiguous LOW
         polygon and return to step 1.
      4) Stop when projected post-fill TotalAc >= target_total_acres, or
         no further contiguous HIGH/MOD/LOW polygon is available.

    Ordering uses arcpy.analysis.Near once before the loop (NEAR_DIST is
    distance from each candidate to the nest point). Per-iteration
    geometry intersection scoring was tried but is too slow at TRPA scale.

    Stop condition: a periodic probe runs the full final pipeline
    (Dissolve + hole-fill + morphological close + re-fill) on a copy of
    the current footprint and measures geodesic acres. This matches the
    reported TotalAc, so the algorithm stops when the result actually
    crosses the target rather than when raw habitat sum does. Probes
    every `probe_every` picks, and every pick once within
    `probe_homestretch_ac` of target.
    """
    with arcpy_temp_manager() as (temps, layers):
        t0 = time.time()

        def step(label):
            log.info(f"  [{time.time() - t0:5.1f}s] {label}")

        # ---- nest layer
        nest_oid_field = arcpy.Describe(nests_fc).OIDFieldName
        nest_lyr = mem("nest")
        arcpy.management.MakeFeatureLayer(
            nests_fc, nest_lyr, f"{nest_oid_field} = {nest_oid}"
        )
        layers.append(nest_lyr)

        # ---- buffer
        buf_fc = mem("buf")
        arcpy.analysis.Buffer(nest_lyr, buf_fc, buffer_dist, dissolve_option="ALL")
        temps.append(buf_fc)
        add_acres(buf_fc)
        buf_ac = sum_field(buf_fc, "Acres")
        need_out = target_total_acres - buf_ac
        step(f"buffer ok ({buf_ac:.1f} ac, need {need_out:.1f} more)")

        # If buffer already meets target, just fill holes + return.
        # (Edge case - 0.25 mi buffer is ~125 ac vs. 500 ac target. Skips
        # the morphological close to avoid trivially inflating an already-
        # over-target buffer.)
        if need_out <= 0:
            final_fc = mem("final")
            fill_holes(buf_fc, final_fc, hole_area_threshold)
            add_acres(final_fc)
            total_ac = sum_field(final_fc, "Acres")
            dt = time.time() - t0
            return final_fc, buf_ac, 0.0, 0.0, 0.0, total_ac, dt

        # ---- pre-filter habitat to polygons within search_radius of the nest
        # so the Erase below runs on a small subset, not the whole basin.
        search_buf = mem("search_buf")
        arcpy.analysis.Buffer(nest_lyr, search_buf, search_radius, dissolve_option="ALL")
        temps.append(search_buf)

        def _local_subset(src_fc, label):
            pre_lyr = mem(f"{label}_pre")
            arcpy.management.MakeFeatureLayer(src_fc, pre_lyr)
            arcpy.management.SelectLayerByLocation(pre_lyr, "INTERSECT", search_buf)
            local_fc = mem(f"{label}_local")
            arcpy.management.CopyFeatures(pre_lyr, local_fc)
            arcpy.management.Delete(pre_lyr)
            temps.append(local_fc)
            return local_fc

        hab_high_local = _local_subset(hab_high, "hab_high")
        step(f"HIGH habitat filtered to within {search_radius} of nest")
        hab_mod_local = _local_subset(hab_mod, "hab_mod")
        step(f"MOD habitat filtered to within {search_radius} of nest")

        # LOW is derived per-nest = (source veg within search radius)
        # minus HIGH minus MOD. HIGH and MOD are exact attribute
        # selections from the same source veg layer; they share the
        # `UniqueID` field, so set difference on UniqueID is equivalent
        # to geometric subtraction.
        #
        # Tried two `Erase` calls (~290 s) and a chunked SQL `NOT IN`
        # WHERE clause (~190 s) - both slow because the search-radius
        # exclude set is ~45K UIDs. Cursor-copy with a Python set is
        # O(rows) with constant-time membership and is orders of
        # magnitude faster than either.
        veg_local = _local_subset(veg_src, "veg")
        step(f"source veg filtered to within {search_radius} of nest")

        exclude_uids: set = set()
        for fc in (hab_high_local, hab_mod_local):
            with arcpy.da.SearchCursor(fc, ["UniqueID"]) as cur:
                for (uid,) in cur:
                    if uid is not None:
                        exclude_uids.add(uid)

        # Empty target FC with same schema as veg_local.
        hab_low_local = mem("hab_low_local")
        out_gdb, out_name_ = os.path.split(hab_low_local)
        arcpy.management.CreateFeatureclass(
            out_gdb, out_name_, "POLYGON",
            template=veg_local,
            spatial_reference=arcpy.Describe(veg_local).spatialReference,
        )
        temps.append(hab_low_local)

        # Cursor-copy non-excluded rows.
        veg_oid_field = arcpy.Describe(veg_local).OIDFieldName
        copy_fields = [
            f.name for f in arcpy.ListFields(veg_local)
            if f.name not in (veg_oid_field, "Shape", "Shape_Length", "Shape_Area")
        ]
        read_fields = copy_fields + ["SHAPE@"]
        uid_idx = copy_fields.index("UniqueID")

        kept = 0
        with arcpy.da.SearchCursor(veg_local, read_fields) as src_cur:
            with arcpy.da.InsertCursor(hab_low_local, read_fields) as ic:
                for row in src_cur:
                    if row[uid_idx] not in exclude_uids:
                        ic.insertRow(row)
                        kept += 1
        step(
            f"LOW derived: {kept} polys kept (excluded {len(exclude_uids)} HIGH+MOD UIDs)"
        )

        # ---- erase buffer from habitat once
        high_work = mem("high_work")
        mod_work = mem("mod_work")
        low_work = mem("low_work")
        arcpy.analysis.Erase(hab_high_local, buf_fc, high_work)
        step("erased buffer from HIGH habitat")
        arcpy.analysis.Erase(hab_mod_local, buf_fc, mod_work)
        step("erased buffer from MOD habitat")
        arcpy.analysis.Erase(hab_low_local, buf_fc, low_work)
        step("erased buffer from LOW habitat")
        temps += [high_work, mod_work, low_work]

        # Planar acres on work layers - 5-10x faster than geodesic and
        # the difference at Tahoe latitudes is noise (<0.5%) at our scale.
        # Output numbers (BufAc, TotalAc) stay geodesic.
        add_acres(high_work, method="AREA")
        add_acres(mod_work, method="AREA")
        add_acres(low_work, method="AREA")
        step("acres calculated on habitat (planar)")

        # Distance to nest, computed once. Basis for greedy ordering.
        arcpy.analysis.Near(high_work, nest_lyr)
        arcpy.analysis.Near(mod_work, nest_lyr)
        arcpy.analysis.Near(low_work, nest_lyr)
        step("near-distance computed")

        high_lyr = mem("high_lyr")
        mod_lyr = mem("mod_lyr")
        low_lyr = mem("low_lyr")
        arcpy.management.MakeFeatureLayer(high_work, high_lyr)
        arcpy.management.MakeFeatureLayer(mod_work, mod_lyr)
        arcpy.management.MakeFeatureLayer(low_work, low_lyr)
        layers += [high_lyr, mod_lyr, low_lyr]

        oid_high = arcpy.Describe(high_work).OIDFieldName
        oid_mod = arcpy.Describe(mod_work).OIDFieldName
        oid_low = arcpy.Describe(low_work).OIDFieldName

        # current_fc starts as the buffer; we APPEND each picked polygon
        # to it directly (no Merge+Dissolve per iteration). The final
        # Dissolve runs once at the end of the loop.
        current_fc = mem("current")
        arcpy.management.CopyFeatures(buf_fc, current_fc)
        temps.append(current_fc)
        step("entering growth loop")

        selected_high: set = set()
        selected_mod: set = set()
        selected_low: set = set()
        high_added = 0.0
        mod_added = 0.0
        low_added = 0.0

        # Probe state. projected_total mirrors what TotalAc would be if we
        # stopped now; updated by maybe_probe(). Initial value = buf_ac
        # since the footprint starts as the buffer.
        projected_total = buf_ac
        picks_since_probe = 0
        # Sliding window of recent probe results - used to detect when
        # picks are happening but the post-fill projected total isn't
        # changing (close eliminates them). When stagnant, slow probe
        # cadence so we don't waste seconds per redundant probe.
        recent_probes: list[float] = []

        # in_fallback flips to True for the optional second growth pass
        # when the strict contiguity loop ran out of candidates without
        # reaching target. Fallback drops the spatial-relation check
        # entirely and just picks the unselected polygon with smallest
        # NEAR_DIST to the nest point. NEAR_DIST is already computed at
        # setup; the 5-mile pre-clip on the layer already enforces the
        # geographic constraint. WITHIN_A_DISTANCE was tried but is too
        # slow at TRPA scale (~50K-poly LOW layer) - same problem as
        # SHARE_A_LINE_SEGMENT_WITH: per-iteration distance/relation
        # against a growing current_fc dominates runtime.
        in_fallback = False

        def pick_best_contiguous(layer, oid_field, selected_set):
            """
            Return the (oid, acres) of the best candidate from `layer`.
            Smallest NEAR_DIST, ties broken by larger Acres. Sub-
            `min_acres` candidates are skipped.

            In strict mode, candidates are first filtered to those that
            satisfy `contiguity_relation` against `current_fc`. In
            fallback mode, no contiguity filter — output may be
            multi-part for sparse-habitat nests.
            """
            if in_fallback:
                # Clear any prior selection - SearchCursor reads all rows.
                arcpy.management.SelectLayerByAttribute(layer, "CLEAR_SELECTION")
            else:
                arcpy.management.SelectLayerByLocation(
                    layer, contiguity_relation, current_fc,
                    selection_type="NEW_SELECTION",
                )

            # In fallback mode, raise the area floor: small disjoint
            # polys get eliminated by the morphological close anyway, so
            # picking them just inflates compute without contributing to
            # TotalAc. Strict mode keeps min_acres low because picks
            # extend the main polygon and survive close trivially.
            ac_floor = fallback_min_acres if in_fallback else min_acres

            best = None  # (near_dist, -acres, oid) - sort min => closest, larger acres
            with arcpy.da.SearchCursor(layer, [oid_field, "Acres", "NEAR_DIST"]) as cur:
                for oid, ac, nd in cur:
                    if oid in selected_set:
                        continue
                    ac = float(ac or 0.0)
                    if ac < ac_floor:
                        continue
                    nd = float(nd if nd is not None else 1e18)
                    cand = (nd, -ac, oid)
                    if best is None or cand < best:
                        best = cand

            if best is None:
                return None
            _, neg_ac, oid = best
            return oid, -neg_ac

        def append_to_footprint(work_fc, oid, oid_field):
            """Read SHAPE@ of one polygon by OID and append it to current_fc."""
            with arcpy.da.SearchCursor(
                work_fc, ["SHAPE@"], where_clause=f"{oid_field} = {oid}"
            ) as cur:
                for (g,) in cur:
                    with arcpy.da.InsertCursor(current_fc, ["SHAPE@"]) as ic:
                        ic.insertRow([g])
                    return

        def _finalize_geometry(input_fc, label):
            """
            Dissolve + interior hole-fill + morphological close + re-fill.

            Returns (out_fc, total_ac, intermediate_fcs). Caller decides
            whether to keep out_fc; intermediate_fcs are always safe to
            delete once total_ac is read.

            The morphological close is a buffer-out then buffer-in by
            close_dist; this collapses concavities and channels narrower
            than 2 * close_dist. Re-fill catches interior holes the close
            may have newly enclosed.
            """
            diss = mem(f"{label}_diss")
            arcpy.management.Dissolve(input_fc, diss)

            pre_close = mem(f"{label}_pre_close")
            fill_holes(diss, pre_close, hole_area_threshold)

            close_out = mem(f"{label}_close_out")
            arcpy.analysis.Buffer(pre_close, close_out, close_dist)

            closed = mem(f"{label}_closed")
            arcpy.analysis.Buffer(close_out, closed, f"-{close_dist}")

            out_fc = mem(f"{label}_out")
            fill_holes(closed, out_fc, hole_area_threshold)
            add_acres(out_fc)  # geodesic - matches reported TotalAc
            total = sum_field(out_fc, "Acres")

            return out_fc, total, [diss, pre_close, close_out, closed]

        def project_total_ac():
            """
            Probe: run the full final pipeline on a copy of current_fc
            and return projected geodesic TotalAc. All intermediates and
            the probe out_fc are deleted before return.
            """
            probe_out, total, probe_temps = _finalize_geometry(current_fc, "probe")
            for t in probe_temps + [probe_out]:
                if arcpy.Exists(t):
                    arcpy.management.Delete(t)
            return total

        def maybe_probe():
            """
            Update projected_total if cadence rules say it's time.

            Cadence:
              - Normally: every `probe_every` picks, OR every pick once
                we're within `probe_homestretch_ac` of target.
              - When the last `stagnation_window` probes all sit within
                `stagnation_tol` ac of each other (no progress), back
                off to once every `stagnation_interval` picks. The
                growth loop keeps running, we just stop wasting ~1-2 s
                on redundant probes.
            """
            nonlocal projected_total, picks_since_probe, recent_probes
            pre_fill = buf_ac + high_added + mod_added + low_added
            in_homestretch = pre_fill >= (target_total_acres - probe_homestretch_ac)

            stagnant = (
                len(recent_probes) >= stagnation_window
                and (max(recent_probes[-stagnation_window:])
                     - min(recent_probes[-stagnation_window:])) < stagnation_tol
            )

            if stagnant:
                interval = stagnation_interval
            else:
                interval = probe_every

            should_probe = (
                (in_homestretch and not stagnant)
                or picks_since_probe >= interval
            )
            if should_probe:
                projected_total = project_total_ac()
                recent_probes.append(projected_total)
                if len(recent_probes) > stagnation_window:
                    recent_probes.pop(0)
                picks_since_probe = 0
                marker = " [stagnant]" if stagnant else ""
                step(
                    f"...probe: projected total = {projected_total:.1f} ac "
                    f"(target {target_total_acres:.1f}){marker}"
                )

        def stopping():
            return projected_total >= target_total_acres

        # ---- Tiered growth loop (HIGH > MOD > LOW), runs once strict,
        # optionally again in fallback mode if still short.
        n_picks = 0

        def grow_phase(label: str):
            nonlocal n_picks, picks_since_probe, high_added, mod_added, low_added
            for _outer in range(max_iters):
                if stopping():
                    break

                # 1) Drain HIGH against the current footprint
                for _inner in range(max_iters):
                    if stopping():
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
                    if n_picks == 1 or n_picks % 25 == 0:
                        step(
                            f"...[{label}] {n_picks} picks | high={high_added:.1f} "
                            f"mod={mod_added:.1f} low={low_added:.1f} "
                            f"projected={projected_total:.1f} / target={target_total_acres:.1f}"
                        )
                    maybe_probe()

                if stopping():
                    break

                # 2) Try ONE MOD, then return to draining HIGH
                pick = pick_best_contiguous(mod_lyr, oid_mod, selected_mod)
                if pick is not None:
                    oid, ac = pick
                    selected_mod.add(oid)
                    mod_added += ac
                    n_picks += 1
                    picks_since_probe += 1
                    append_to_footprint(mod_work, oid, oid_mod)
                    step(f"...[{label}] MOD pick #{n_picks}: oid={oid} +{ac:.1f} ac")
                    maybe_probe()
                    continue  # back to drain HIGH against the new footprint

                # 3) No MOD touches -> try ONE LOW, then return to HIGH+MOD
                pick = pick_best_contiguous(low_lyr, oid_low, selected_low)
                if pick is None:
                    break  # nothing in any tier touches the footprint
                oid, ac = pick
                selected_low.add(oid)
                low_added += ac
                n_picks += 1
                picks_since_probe += 1
                append_to_footprint(low_work, oid, oid_low)
                step(f"...[{label}] LOW pick #{n_picks}: oid={oid} +{ac:.1f} ac")
                maybe_probe()

        # Phase 1: strict contiguity (INTERSECT by default).
        grow_phase("strict")

        # Phase 2: sparse-habitat fallback. Only fires if Phase 1 ended
        # short. Picks nearest unselected polygons by NEAR_DIST without
        # any contiguity check (geographic bound is the 5-mile pre-clip
        # already in effect on each layer).
        if not stopping():
            picks_before_fallback = n_picks
            in_fallback = True
            recent_probes.clear()  # stagnation tracker re-baselines
            step(
                f"sparse-habitat fallback: nearest-by-NEAR_DIST, no contiguity"
                f" (target {target_total_acres:.1f}, projected {projected_total:.1f})"
            )
            grow_phase("fallback")
            step(f"fallback added {n_picks - picks_before_fallback} picks")

        step(f"growth done: {n_picks} picks, finalizing geometry")

        # Final pipeline: Dissolve + hole-fill + close + re-fill.
        # _finalize_geometry uses the same steps the probe used, so the
        # reported TotalAc matches the value that triggered the stop.
        final_fc, total_ac, final_temps = _finalize_geometry(current_fc, "final")
        temps.extend(final_temps)

        dt = time.time() - t0
        return final_fc, buf_ac, high_added, mod_added, low_added, total_ac, dt


# -------------------
# MAIN
# -------------------
def main() -> None:
    _setup_logging()
    # Wipe any leftover in_memory state from a prior aborted run in this
    # Python process. Per-nest temps are still tracked + deleted by the
    # context manager; this is just a safety net.
    clear_in_memory()

    # -------- USER INPUTS --------
    # Source workspace - read-only, where input feature classes live.
    source_gdb = r"C:\GIS\Scratch.gdb"
    arcpy.env.workspace = source_gdb

    nests = "nogo_nest_usfs"
    hab_high = "amgo_ecobject_cwhr_habmodel_high"
    hab_mod = "amgo_ecobject_cwhr_habmodel_moderate"
    # LOW is derived per-nest from the source veg layer (no precomputed
    # LOW FC exists). Lives outside the working geodatabase.
    veg_src = r"C:\GIS\TahoeMaps\Tahoe_Data.gdb\Vegetation_Ecobject_2010"

    # Dedicated output workspace - keeps goshawk outputs separate from
    # Scratch.gdb so we don't pollute it with per-run artifacts. Auto-
    # created if missing.
    output_gdb = r"C:\GIS\Goshawk_Threshold_Zone.gdb"
    base_out_name = "TRPA_Goshawk_Threshold_Zone"

    # ---- TEST MODE ----
    # Exactly one of these can be set to limit a run; both None => full
    # 136-nest production run.
    #   TEST_OIDS = [...]  - run only these specific nest OIDs.
    #   TEST_N    = N      - run the first N nests in source order.
    # Either toggle suffixes the output FC name so the production output
    # (TRPA_Goshawk_Threshold_Zone) is never clobbered by a test run.
    # FULL PRODUCTION RUN: both toggles None.
    TEST_OIDS = None
    TEST_N = None
    TEST_LABEL = "FIXES"  # only used when TEST_OIDS is set

    if TEST_OIDS is not None:
        out_name = f"{base_out_name}_TEST_{TEST_LABEL}"
    elif TEST_N is not None:
        out_name = f"{base_out_name}_TEST_N{TEST_N}"
    else:
        out_name = base_out_name
    out_fc = output_gdb + "\\" + out_name

    target_total_acres = 500.0
    buffer_dist = "0.25 Miles"

    # Tunables
    hole_area_threshold = 10 ** 18  # huge -> fills ALL interior holes
    max_iters = 5000                # safety cap on growth iterations
    min_acres = 2.0                 # strict-mode floor: skip habitat polygons smaller than this
    fallback_min_acres = 5.0        # fallback-mode floor: higher because disjoint picks must survive close on their own
    search_radius = "5 Miles"       # pre-filter habitat per nest before Erase
    close_dist = "200 Feet"         # morphological close radius (closes gaps <= 2x this)
    probe_every = 5                 # picks between projected-total probes
    probe_homestretch_ac = 50.0     # within X ac of target, probe every pick

    # Per-iteration pick contiguity. INTERSECT is fast (~3-4x faster
    # than SHARE_A_LINE_SEGMENT_WITH on TRPA's data sizes) at the cost
    # of admitting some point-touch picks that the close pass
    # eliminates. min_acres=2.0 + the stagnation guard keep that ghost-
    # pick cost bounded.
    contiguity_relation = "INTERSECT"
    # Sparse-habitat fallback: if the strict contiguity loop runs out
    # of candidates without reaching target, run a second pass with
    # WITHIN_A_DISTANCE relaxed contiguity at this radius.
    fallback_search_distance = "0.5 Miles"
    # Stagnation guard on probe cadence: when the last `stagnation_window`
    # probes all sit within `stagnation_tol` ac, slow probe cadence to
    # `stagnation_interval` picks until something moves.
    stagnation_window = 5
    stagnation_tol = 0.5
    stagnation_interval = 10

    log.info("=" * 60)
    log.info("TRPA Goshawk Threshold Zone")
    log.info("=" * 60)
    log.info(f"Source GDB:   {source_gdb} (read-only)")
    log.info(f"Output GDB:   {output_gdb}")
    log.info(f"Nests FC:     {nests}")
    log.info(f"HIGH habitat: {hab_high}")
    log.info(f"MOD habitat:  {hab_mod}")
    log.info(f"Veg source:   {veg_src} (LOW = veg - HIGH - MOD per-nest)")
    log.info(f"Output FC:    {out_fc}")
    if TEST_OIDS is not None:
        log.info(f"TEST mode:    OIDs={TEST_OIDS} (label={TEST_LABEL})")
    elif TEST_N is not None:
        log.info(f"TEST mode:    first {TEST_N} nest(s)")
    else:
        log.info("TEST mode:    OFF (full production run)")
    log.info(f"Target ac:    {target_total_acres} | Buffer: {buffer_dist} | min_acres: {min_acres} | search: {search_radius}")
    log.info(f"Close dist:   {close_dist} | probe every: {probe_every} picks | homestretch: {probe_homestretch_ac} ac")
    log.info(f"Contiguity:   {contiguity_relation} | fallback: nearest-by-NEAR_DIST (no contiguity)")
    log.info(f"Stagnation:   window={stagnation_window} tol={stagnation_tol} ac slow_interval={stagnation_interval} picks")

    # -------- Create output GDB + FC --------
    ensure_file_gdb(output_gdb)
    sr = arcpy.Describe(hab_high).spatialReference
    if arcpy.Exists(out_fc):
        log.info(f"Replacing existing {out_fc}")
        arcpy.management.Delete(out_fc)

    arcpy.management.CreateFeatureclass(output_gdb, out_name, "POLYGON", spatial_reference=sr)
    for fn, ft in [
        ("NestOID", "LONG"),
        ("BufAc", "DOUBLE"),
        ("HighAc", "DOUBLE"),
        ("ModAc", "DOUBLE"),
        ("LowAc", "DOUBLE"),
        ("FilledAc", "DOUBLE"),
        ("TotalAc", "DOUBLE"),
    ]:
        arcpy.management.AddField(out_fc, fn, ft)

    # -------- Get nest OIDs --------
    nest_oid_field = arcpy.Describe(nests).OIDFieldName
    all_oids = [r[0] for r in arcpy.da.SearchCursor(nests, [nest_oid_field])]
    if TEST_OIDS is not None:
        wanted = set(TEST_OIDS)
        nest_oids = [o for o in all_oids if o in wanted]
        missing = wanted - set(nest_oids)
        if missing:
            log.warning(f"TEST_OIDS not found in {nests}: {sorted(missing)}")
        log.info(f"TEST MODE: running {len(nest_oids)} specific OID(s): {nest_oids}")
    elif TEST_N is not None:
        nest_oids = all_oids[:TEST_N]
        log.info(f"TEST MODE: limited to first {len(nest_oids)} nest(s): {nest_oids}")
    else:
        nest_oids = all_oids
        log.info(f"Nests found: {len(nest_oids)}")

    # -------- Process nests --------
    failures = 0
    short_nests: list[int] = []
    t_start = time.time()

    for i, nest_oid in enumerate(nest_oids, start=1):
        log.info(f"[{i}/{len(nest_oids)}] Nest OID {nest_oid}")
        try:
            final_fc, buf_ac, high_added, mod_added, low_added, total_ac, dt = build_threshold_zone_for_nest(
                nests_fc=nests,
                nest_oid=nest_oid,
                hab_high=hab_high,
                hab_mod=hab_mod,
                veg_src=veg_src,
                buffer_dist=buffer_dist,
                target_total_acres=target_total_acres,
                hole_area_threshold=hole_area_threshold,
                max_iters=max_iters,
                min_acres=min_acres,
                fallback_min_acres=fallback_min_acres,
                search_radius=search_radius,
                close_dist=close_dist,
                probe_every=probe_every,
                probe_homestretch_ac=probe_homestretch_ac,
                contiguity_relation=contiguity_relation,
                fallback_search_distance=fallback_search_distance,
                stagnation_window=stagnation_window,
                stagnation_tol=stagnation_tol,
                stagnation_interval=stagnation_interval,
            )

            filled_ac = max(0.0, total_ac - (buf_ac + high_added + mod_added + low_added))

            with arcpy.da.InsertCursor(
                out_fc,
                ["SHAPE@", "NestOID", "BufAc", "HighAc", "ModAc", "LowAc", "FilledAc", "TotalAc"],
            ) as ic:
                for (geom,) in arcpy.da.SearchCursor(final_fc, ["SHAPE@"]):
                    ic.insertRow([
                        geom, nest_oid,
                        float(buf_ac), float(high_added), float(mod_added),
                        float(low_added), float(filled_ac), float(total_ac),
                    ])

            try:
                if arcpy.Exists(final_fc):
                    arcpy.management.Delete(final_fc)
            except Exception as e:
                log.warning(f"cleanup of final_fc failed: {type(e).__name__}: {e}")

            short = total_ac < (target_total_acres - 0.01)
            if short:
                short_nests.append(nest_oid)

            log.info(
                f"  Buffer: {buf_ac:.2f} | High(add): {high_added:.2f} | "
                f"Mod(add): {mod_added:.2f} | Low(add): {low_added:.2f} | "
                f"Filled: {filled_ac:.2f} | Total: {total_ac:.2f} | {dt:.1f}s"
                + ("  WARNING: Could not reach target acres contiguously." if short else "")
            )

        except Exception:
            failures += 1
            log.error(f"ERROR on nest {nest_oid}")
            log.error(traceback.format_exc())
            try:
                arcpy.ClearWorkspaceCache_management()
            except Exception as e:
                log.warning(f"ClearWorkspaceCache failed: {type(e).__name__}: {e}")

    # -------- End-of-run summary --------
    total_dt = time.time() - t_start
    log.info("=" * 60)
    log.info(f"Done. Output: {out_fc}")
    log.info(
        f"Nests processed: {len(nest_oids)} | "
        f"failures: {failures} | "
        f"short of target: {len(short_nests)} | "
        f"elapsed: {total_dt:.1f}s"
    )
    if short_nests:
        log.info(f"Short OIDs: {short_nests}")

    # Final in_memory wipe - belt-and-suspenders, even though per-nest
    # arcpy_temp_manager already cleans up tracked temps individually.
    clear_in_memory()
    log.info("in_memory cleared")


if __name__ == "__main__":
    main()
