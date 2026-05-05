# Methods: TRPA Goshawk Threshold Zone

**Owner:** mbindl@trpa.gov
**Last reviewed:** 2026-04-29

## Purpose

For each Northern Goshawk (*Accipiter gentilis*) nest in the TRPA basin
this pipeline produces a single polygon — the **Goshawk Threshold Zone**
— that represents the protected area used for threshold/wildlife
planning. The zone is built as a 0.25-mile nest buffer plus enough
**contiguous** suitable habitat to reach a target of **500 acres
total**, with interior holes filled and boundary concavities collapsed
by a small morphological close.

If a nest is in genuinely sparse habitat (water, urban, or rock
surrounds), the zone is the buffer + whatever HIGH/MOD habitat reached
contiguously. Such nests are flagged short and listed in the run
summary. We do not reach across miles of non-habitat to manufacture a
500-ac zone.

The output feeds threshold-evaluation reporting and project-review
screening — any project intersecting a goshawk threshold zone triggers
wildlife review.

## Data sources

| Source | Type | Refresh | Notes |
|---|---|---|---|
| `nogo_nest_usfs` | Feature class (points) | Manual, USFS releases | Authoritative goshawk nest locations. Has `NEST_ID` (string, e.g. `'2011A'`) and `LOCATION` (e.g. `'Sierra Creek'`) attributes used in the output. |
| `amgo_ecobject_cwhr_habmodel_high` | Feature class (polygons) | With CWHR/ecobject updates | HIGH-suitability goshawk habitat per the CWHR habitat model on the ecobject layer. ~113K polys basin-wide. |
| `amgo_ecobject_cwhr_habmodel_moderate` | Feature class (polygons) | With CWHR/ecobject updates | MODERATE-suitability habitat. Used only when contiguous HIGH is exhausted. ~281K polys basin-wide. |

All three live in `C:\GIS\Scratch.gdb` (read-only source). The pipeline
output goes to a **dedicated** geodatabase:

- **Output GDB:** `C:\GIS\Goshawk_Threshold_Zone.gdb` (auto-created on
  first run; contains `TRPA_Goshawk_Threshold_Zone` plus any per-nest
  `*_<NEST_ID>` test outputs from notebook spot-checks).

The dedicated output GDB exists so per-run artifacts don't pollute
`Scratch.gdb`. The spatial reference of
`amgo_ecobject_cwhr_habmodel_high` is reused for the output. Reported
output acres (`BufAc`, `TotalAc`) are **geodesic**.

There is no LOW tier and no per-nest derivation from the broader
`Vegetation_Ecobject_2010` veg layer in v1. Earlier iterations tried
that approach and it kept pulling water/urban/rock polygons into
threshold zones (a single 1411-ac lake polygon got picked once). The
CWHR HIGH/MOD ratings are the goshawk-habitat definition; anything not
rated is not goshawk habitat.

## Processing steps

The pipeline lives in
[goshawk_threshold_zone.py](goshawk_threshold_zone.py) (promoted
runner) and the synced authoring notebook
[data_engineering.ipynb](data_engineering.ipynb). Both implement the
same algorithm — the notebook is structured cell-by-cell so a single
nest can be inspected in ArcGIS Pro between steps.

For each nest:

1. **Buffer.** 0.25-mile geodesic buffer around the nest point
   (`arcpy.analysis.Buffer`, `dissolve_option="ALL"`).
2. **Pre-clip habitat to a 5-mile envelope.** Buffer the nest by
   `search_radius` (default 5 miles), then `SelectLayerByLocation` +
   `CopyFeatures` HIGH and MOD into small `in_memory` subsets. Without
   this every per-nest `Erase` would run against the full basin layers
   and dominate runtime.
3. **Erase.** Erase the buffer footprint from each pre-clipped subset
   so habitat overlapping the buffer isn't double-counted.
4. **Acres + Near.** `add_acres(method="AREA_GEODESIC")` and
   `arcpy.analysis.Near` (distance from each polygon to the nest
   point) on each work layer. Distance is computed once before the
   growth loop; ordering is `NEAR_DIST` ascending, ties broken by
   larger `Acres`.
5. **Tiered growth with probe-based stop.** `INTERSECT` contiguity
   against the in-progress `current_fc`:
   - **Phase 1 — drain HIGH.** Repeatedly add the HIGH polygon
     closest to the nest that touches `current_fc`. Each pick's
     `SHAPE@` is appended to `current_fc` via `InsertCursor` — no
     Merge/Dissolve per iteration.
   - **Phase 2 — tiered HIGH → ONE MOD.** If still short, take one
     contiguous MOD pick, then re-check HIGH (the new footprint may
     now touch HIGH that didn't reach before). Repeat until target
     met or no contiguous candidate in either tier.

   **Stop condition is post-close projected `TotalAc`.** Every
   `probe_every` picks (default 5), and every pick once the pre-fill
   running sum is within `probe_homestretch_ac` of target (default
   100), the algorithm runs the full final pipeline (Dissolve + hole-
   fill + close + re-fill) on a copy of `current_fc` and measures
   geodesic acres. Growth stops as soon as the projected total
   reaches `target_ac`. Without this, the morphological close adds
   30–280 ac of "Filled" on top of the pre-fill running sum and the
   final TotalAc systematically overshoots by that amount.

   Sub-`min_acres` candidates (default 0.5 ac) are skipped.

6. **Finalize.** Same pipeline the probe used:
   1. `Dissolve` the buffer + appended polygons into one geometry.
   2. `EliminatePolygonPart` with `condition="AREA"`,
      `part_area=10**18`, `part_option="CONTAINED_ONLY"` — fills
      INTERIOR holes only. Multipart exterior parts are preserved
      (the buffer is never silently eliminated).
   3. **Morphological close.** `Buffer` outward by `close_dist`
      (default 200 ft), then `Buffer` inward by the same distance.
      Collapses concavities, narrow channels, and "non-hole" gaps
      narrower than `2 * close_dist` (~400 ft at default) — fixes
      the spider-with-thin-reaches shapes the greedy-contiguous
      algorithm produces in fragmented habitat.
   4. Re-run `EliminatePolygonPart` (still `CONTAINED_ONLY`) to
      catch any interior holes the close just enclosed.
7. **Output row.** Write one feature with `NestID`, `NestOID`,
   `Location`, `BufAc`, `HighAc`, `ModAc`, `TotalAc`, `Picks`, `Sec`,
   `Short` to `TRPA_Goshawk_Threshold_Zone`.

After the per-nest build loop completes, a postprocess **trim** runs
automatically (controlled by `DO_TRIM = True` in `main()`):

8. **Identity overlay vs. source veg.** `arcpy.analysis.Identity` of
   the threshold-zone FC × `Vegetation_Ecobject_2010` →
   `TRPA_GTZ_x_Veg`. Each output piece carries the threshold zone's
   `NestID`/`Location`/etc. plus the underlying veg attributes. Add a
   geodesic `PieceAc` field on the result.
9. **Per-piece distance to parent nest.** For each Identity piece,
   compute `geom.distanceTo(nest_geom)` against its own parent nest
   (looked up by `NestOID`). Buffer pieces are at distance 0; outermost
   close-fill bits and far habitat are largest distance. Group pieces
   by `NestID`, sort each group ascending by distance.
10. **Trim cumulative.** For each zone with `TotalAc > target_ac`,
    walk its sorted piece list and cumulative-sum `PieceAc`. Stop
    *before* the next piece would push past target — drop that piece
    and everything beyond. Buffer pieces are always at the head of the
    sorted list, so they're always kept; every trimmed zone still
    contains its nest.
11. **Dissolve + write trimmed FC.** Dissolve the kept pieces per
    zone, recompute geodesic acres, write to
    `TRPA_Goshawk_Threshold_Zone_Trimmed`. Schema is the original
    threshold-zone schema plus `OrigAc` (pre-trim `TotalAc`) and
    `Trimmed` (1 if this row was trimmed, 0 if it was already at or
    below target). Under-target zones pass through unchanged with
    `Trimmed = 0`, `OrigAc = TotalAc`.

The trimmed FC is the canonical "no zone over 500 ac" deliverable.
The raw `TRPA_Goshawk_Threshold_Zone` and the Identity intermediate
`TRPA_GTZ_x_Veg` are kept in the output GDB for audit / further
analysis (the notebook's §12 fire-severity overlay reuses
`TRPA_GTZ_x_Veg`).

## Key assumptions

- **"Suitable habitat" = HIGH ∪ MOD** classes from the CWHR habitat
  model on the ecobject layer. HIGH preferred over MOD; MOD used only
  when no contiguous HIGH remains. There is no LOW tier and no
  fallback to `Vegetation_Ecobject_2010` (see Changelog 2026-04-29 for
  why earlier attempts were removed).
- **Contiguity is required.** `SelectLayerByLocation(..., "INTERSECT",
  current_fc)`. Polygons that don't touch the growing footprint —
  even if closer to the nest than already-picked patches — are not
  included. This is what makes the "many nests not buffered" bug
  impossible: the buffer is always at the center of a single
  connected polygon, never a disjoint piece that could be eliminated
  by hole-fill.
- **"Closest" candidate = smallest `NEAR_DIST` to the nest**, ties
  broken by larger Acres. Distance is to the nest *point*, computed
  once before the loop. Per-iteration footprint-relative shared-
  boundary metrics were tried earlier and are too slow at TRPA scale.
- **Stop condition is post-close projected `TotalAc`**, not pre-fill
  sum. Periodic probe of the full final pipeline. Replaces an earlier
  pre-fill stop that overshot by 30–280 ac when habitat had
  concavities for the close to fill.
- **Morphological close** at `close_dist = "200 Feet"` collapses
  concavities ≤ ~400 ft. Tunable in the user-inputs block; raising it
  bridges wider gaps but risks fabricating area across legitimate
  non-habitat (roads, riparian corridors, narrow water).
- **`fill_interior_holes` uses `part_option="CONTAINED_ONLY"`.** This
  eliminates only INTERIOR holes (holes inside the polygon).
  Multipart exterior parts are preserved. The earlier code used
  `"ANY"`, which silently deletes every multipart part except the
  largest one — that bug caused several nests' buffers to disappear.
- **Sliver filter** at `min_acres = 0.5` ac. Tunable.
- **Search radius pre-filter.** Habitat is pre-clipped per-nest to
  `search_radius = "5 Miles"` before `Erase`. Polygons farther than
  5 mi are not eligible. Tunable; raise if a future nest in unusual
  habitat is flagged short.
- **Geodesic acres throughout.** `BufAc`, `HighAc`, `ModAc`, and
  `TotalAc` are all geodesic. Earlier versions split this and reported
  planar acres on work layers for speed; in v1 the pipeline is fast
  enough without that split.

## Output schemas

### `TRPA_Goshawk_Threshold_Zone` (raw build, may be over target)

| Field | Type | Meaning |
|---|---|---|
| `NestID` | TEXT(64) | `NEST_ID` from `nogo_nest_usfs` (e.g. `'2011A'`). Source-of-truth identifier. |
| `NestOID` | LONG | `OBJECTID` of the source nest. Stable per-row pointer. |
| `Location` | TEXT(128) | `LOCATION` field from the nest layer (e.g. `'Sierra Creek'`). Helpful for human review. |
| `BufAc` | DOUBLE | Geodesic acres in the 0.25-mile buffer (~125.5). |
| `HighAc` | DOUBLE | Geodesic acres of HIGH habitat added (post-erase). |
| `ModAc` | DOUBLE | Geodesic acres of MOD habitat added (post-erase). |
| `TotalAc` | DOUBLE | Geodesic acres of the final polygon (post-dissolve, post-fill, post-close, post-re-fill). |
| `Picks` | LONG | Number of habitat polygons added during growth. |
| `Sec` | DOUBLE | Per-nest runtime in seconds (diagnostic). |
| `Short` | SHORT | 1 if `TotalAc < target − 0.01`, else 0. |

The catch-all "Filled" / "LowAc" columns from earlier iterations are
removed in v1. The math `BufAc + HighAc + ModAc ≤ TotalAc` is the
expected relation; any difference is the close + interior-hole-fill
contribution and isn't broken out separately.

### `TRPA_Goshawk_Threshold_Zone_Trimmed` (postprocess output, all rows ≤ target)

Same schema as the raw FC, plus two audit fields:

| Field | Type | Meaning |
|---|---|---|
| `OrigAc` | DOUBLE | Pre-trim `TotalAc` from the raw FC. For rows where `Trimmed = 0`, equals `TotalAc`. |
| `Trimmed` | SHORT | 1 if this row was trimmed by the postprocess, 0 if it was already ≤ target. |

`TotalAc` in the trimmed FC is recomputed (geodesic) on the dissolved
kept-pieces polygon. By construction, no row has `TotalAc > target`.

### `TRPA_GTZ_x_Veg` (Identity overlay intermediate, kept for audit)

`arcpy.analysis.Identity(threshold_fc, veg_src)` output. One row per
threshold-zone × veg-polygon intersection. Carries the threshold
zone's full schema plus all veg attributes (`WHRTYPE`, `WHRSIZE`,
`WHRDENSITY`, `CWHR`, `COVERTYPE`, `WHRLIFEFORM`, etc.) and a geodesic
`PieceAc`. The trim postprocess uses this; the notebook's §12
fire-severity overlay also reuses it.

## Known caveats

- **Sparse-habitat shorts.** Nests genuinely surrounded by water, rock,
  or urban (e.g., several near Lake Tahoe's south shore) cannot reach
  500 ac contiguously. Their threshold zone is buffer + max contiguous
  habitat reached, with `Short = 1` and a `WARNING` log line. Reviewer
  judgment on these nests; raising `search_radius` rarely helps because
  the problem is the local landscape, not the search envelope.
- **Greedy growth is not optimal.** The algorithm picks the locally
  best next polygon. Produces compact, defensible shapes but is not
  guaranteed to land exactly on 500 ac. With the probe stop, residual
  overshoot is the size of the last-pick "jump" the probe didn't
  catch — typically 0–30 ac.
- **Close-distance trade-off.** A 200-ft close fills concavities ≤
  ~400 ft. This can bridge legitimate gaps if the local landscape has
  roads, riparian corridors, or narrow water bodies in that range. If
  a reviewer flags a zone for spanning a non-habitat feature, lower
  `close_dist` for that nest (or globally) and re-run.
- **Erased habitat slivers.** When a habitat polygon partially
  overlaps the buffer, `Erase` clips it to the area outside. Only the
  outside-the-buffer remainder is eligible. The original overlap is
  already counted in `BufAc`.
- **Negative buffer requires projected SR.** The morphological close
  uses `arcpy.analysis.Buffer(..., "-200 Feet")`. The source layer's
  spatial reference (reused for output) must be projected for this to
  behave correctly. TRPA basin layers typically are. A geographic SR
  would behave unpredictably.
- **In-memory growth.** Per-nest temps live in `in_memory` and are
  wiped between nests by `clear_in_memory()`. The runner also wipes
  `in_memory` at start (in case a prior aborted run left state) and
  end of `main()`.

## Run scope and performance

Runs over 136 goshawk nests in `nogo_nest_usfs`. No parallelism. Per-
nest timing is logged. Expected total runtime in v1 is ~5–7 hours.
Most of that is the per-nest geometry pipeline (Erase against the
~5-mile habitat clip is the largest chunk); probe overhead is small
(~1–3 s per probe, ~5–15 probes per nest).

If runtime ever becomes a problem, drop `search_radius` to `"3 Miles"`
in the user-inputs block. Roughly halves the post-clip polygon count
with negligible impact on the result (any nest needing habitat farther
than 5 mi is already going to flag short).

### Notebook test mode

[data_engineering.ipynb](data_engineering.ipynb) is structured for
single-nest cell-by-cell testing. Set `NEST_ID` in the configuration
cell (cell 4), then step through cells 6 → 18 to inspect each
geometry stage in ArcGIS Pro. Test outputs go to per-nest FCs named
`TRPA_Goshawk_Threshold_Zone_<NEST_ID>` in the output GDB so they
don't clobber the production output. The notebook also has an
all-nests cell at the bottom (mirrors what the .py does); for
production use the .py.

## Downstream consumers

- TRPA threshold-evaluation reporting (Wildlife indicator).
- Project-review screening: any project intersecting a threshold zone
  triggers wildlife review.

## Changelog

### 2026-05-01 — trim postprocess

The probe-based stop catches most overshoots, but the morphological
close still pushes some nests over (the close adds 30–280 ac on top
of the picked habitat for nests with concave/fragmented habitat). On
the most recent full run, ~79/136 zones came in over 500 ac.

Added a postprocess **trim** that runs automatically after the build
loop in `goshawk_threshold_zone.py` (`DO_TRIM = True` in `main()`):

- New `trim_zones_to_target()` function. Steps: Identity threshold ×
  veg → `TRPA_GTZ_x_Veg` with geodesic `PieceAc`; per-piece distance
  to parent nest (Python `geom.distanceTo`); group/sort pieces per
  nest; cumulative-sum until next piece would push past target;
  dissolve kept pieces; recompute geodesic acres; write
  `TRPA_Goshawk_Threshold_Zone_Trimmed`.
- Buffer pieces are at distance 0 → always at the head of the sorted
  list → always kept. Every trimmed zone still contains its nest.
- Output FC adds `OrigAc` (pre-trim `TotalAc`) and `Trimmed` (1/0)
  for audit. Under-target rows pass through with original geometry,
  `Trimmed = 0`.
- Notebook mirrors this as §13 with cell-by-cell visibility.

The trimmed FC is the canonical "no zone over 500 ac" deliverable.
The raw FC and the Identity intermediate are kept in the output GDB
for review and downstream analysis.

### 2026-04-29 — v1 simplification

After two days of compounding workarounds (LOW-tier derivation,
sparse-habitat fallback with `WITHIN_A_DISTANCE` then `NEAR_DIST`-only,
stagnation guards, `min_acres` bumps, ghost-pick filtering), the
pipeline was producing biologically wrong outputs — threshold zones
covering lakes, several nests not even inside their own threshold
zone, single-pick overshoots up to 1598 ac. Stripped back to a simple
defensible algorithm:

- **Removed LOW tier and the per-nest `Vegetation_Ecobject_2010`
  derivation.** The CWHR HIGH/MOD ratings are the goshawk-habitat
  definition; treating "anything else in the veg layer" as a fallback
  pulled in water, urban, rock, etc.
- **Removed the sparse-habitat fallback pass** (relaxed contiguity).
  Nests that can't reach 500 ac with strict contiguous HIGH+MOD are
  reported short. That's the biologically defensible answer for nests
  surrounded by non-habitat.
- **Fixed `fill_interior_holes`** to use `part_option="CONTAINED_ONLY"`.
  The earlier `"ANY"` with a 10**18 area threshold silently dropped
  every multipart exterior part except the largest — the bug behind
  several nests not being covered by their threshold zone.
- **Kept the morphological close + probe-based stop.** Close fixes the
  spider-with-thin-reaches greedy-growth shapes; probe stops growth
  when projected post-close TotalAc reaches target (otherwise the
  close adds 30–280 ac of unaccounted overshoot).
- **Output schema simplified.** Dropped `LowAc` and the catch-all
  `FilledAc`. Added `NestID` (string from `NEST_ID` field, source-of-
  truth), `Location`, `Picks`, `Sec`, `Short` for self-identifying
  audit-friendly rows.
- **Notebook restructured** as cell-by-cell for single-nest testing
  with a `NEST_ID` parameter, plus an all-nests loop cell at the
  bottom.

### 2026-04-28 — workarounds (mostly removed in v1)

- Added LOW-suitability tier derived from `Vegetation_Ecobject_2010`
  (later removed — pulled in water/urban).
- Added relaxed-contiguity fallback (later removed — produced disjoint
  multipart outputs whose buffers were silently dropped by the
  `"ANY"` hole-fill bug).
- Switched output FC to `C:\GIS\Goshawk_Threshold_Zone.gdb` (kept).
- Added `clear_in_memory()` at start/end of `main()` (kept).
- Added LOW derivation via cursor-copy (removed with LOW tier).
- Added probe-based stop (kept; revised in v1 to operate on the
  simpler single-connected-polygon current_fc).
- Added morphological close (kept).
- Added `TEST_OIDS`/`TEST_N`/`TEST_LABEL` toggles (replaced in v1 by
  per-`NEST_ID` notebook testing).

### 2026-04-27 — initial promotion

- Initial methods documentation.
- Algorithm correctness pass: tiered HIGH→MOD priority,
  `min_acres` sliver filter.
- Removed the older procedural cell from the notebook.
- Promoted the notebook to `goshawk_threshold_zone.py` with stdlib
  logging.
- Per-nest 5-mile pre-clip on HIGH/MOD, append-not-dissolve growth
  pattern, planar acres on work layers.
