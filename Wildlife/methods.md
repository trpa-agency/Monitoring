# Methods: TRPA Goshawk Threshold Zone

**Owner:** masonbindl@gmail.com
**Last reviewed:** 2026-04-28

## Purpose

For each Northern Goshawk (*Accipiter gentilis*) nest in the TRPA basin, this
pipeline produces a single polygon — the **Goshawk Threshold Zone** — that
represents the protected area used for threshold/wildlife planning. The zone
is built as a 0.25-mile nest buffer plus enough **contiguous** suitable
habitat to reach a target of **500 acres total**, with all interior holes
filled. Sparse-habitat nests fall back to a relaxed
`WITHIN_A_DISTANCE` second pass (multi-part output) so they can still
reach target where strict contiguity runs out. Output is one row per
nest with components broken out for audit.

The output feeds threshold-evaluation reporting and project-review screening
where activity inside a goshawk threshold zone triggers special review.

## Data sources

| Source | Type | Refresh | Notes |
|---|---|---|---|
| `nogo_nest_usfs` | Feature class (points) | Manual, USFS releases | Authoritative goshawk nest locations |
| `amgo_ecobject_cwhr_habmodel_high` | Feature class (polygons) | With CWHR/ecobject updates | HIGH-suitability habitat per the CWHR habitat model on the ecobject layer. ~113K polys basin-wide. |
| `amgo_ecobject_cwhr_habmodel_moderate` | Feature class (polygons) | With CWHR/ecobject updates | MODERATE-suitability habitat (used only when contiguous HIGH is exhausted). ~281K polys basin-wide. |
| `Vegetation_Ecobject_2010` | Feature class (polygons) | Periodic re-derivation from CalVeg/USFS | Source veg layer that the HIGH and MOD layers were derived from. Complete basin coverage (~700K polys). LOW = (this layer within search radius) − HIGH − MOD, derived per-nest. |

The first three input layers live in `C:\GIS\Scratch.gdb` (read-only
source). `Vegetation_Ecobject_2010` lives in
`C:\GIS\TahoeMaps\Tahoe_Data.gdb` (read-only source). The pipeline
output goes to a **dedicated** geodatabase:

- **Output GDB:** `C:\GIS\Goshawk_Threshold_Zone.gdb`
  (auto-created on first run; contains `TRPA_Goshawk_Threshold_Zone`
  and any `*_TEST_N{N}` / `*_TEST_<LABEL>` test outputs).

The dedicated output GDB exists so per-run artifacts don't pollute
`Scratch.gdb`. The spatial reference of
`amgo_ecobject_cwhr_habmodel_high` is reused for the output. Reported
output acres (`BufAc`, `TotalAc`) are **geodesic**.

There is no precomputed `_low` feature class — LOW is derived on the
fly per nest, so any vegetated polygon that the CWHR model didn't
classify as HIGH or MOD goshawk habitat is eligible as a last-resort
fallback. This deliberately includes some non-forested types
(meadows, riparian, etc.); they're filtered out implicitly by the
contiguity requirement and `min_acres` sliver filter.

## Processing steps

The whole pipeline is in [data_engineering.ipynb](data_engineering.ipynb)
(authoring) and [goshawk_threshold_zone.py](goshawk_threshold_zone.py)
(promoted runner). Both are kept in sync.

For each nest OID:

1. **Buffer.** Make a 0.25-mile geodesic buffer around the nest point
   (`arcpy.analysis.Buffer`, `dissolve_option="ALL"`).
2. **Short-circuit.** If the buffer alone already ≥ 500 ac, fill interior
   holes and write that as the threshold zone.
3. **Pre-clip habitat to a search envelope.** Buffer the nest by
   `search_radius` (default 5 miles), and `SelectLayerByLocation`-then-
   `CopyFeatures` each of HIGH, MOD, and the source veg layer into small
   `in_memory` subsets. Everything downstream operates on the subset.
   The veg layer is especially important to clip — it's ~700K polygons
   basin-wide and would dominate runtime if used directly.

   **Derive LOW.** HIGH and MOD are exact attribute selections from the
   same source veg layer, so they share a `UniqueID` field. We collect
   `UniqueID`s from the local HIGH and MOD subsets into a Python set,
   create an empty target FC with `template=veg_local` (matches schema),
   and `SearchCursor` → `InsertCursor` row-copy from `veg_local`,
   skipping any row whose `UniqueID` is in the exclude set. O(rows)
   with constant-time set membership. Two earlier approaches were
   tried and discarded:
   - Two `Erase` calls (~290 s/nest): geometric subtraction is
     overkill when set difference on UniqueID is equivalent.
   - Single chunked `UniqueID NOT IN (…)` SQL WHERE clause (~190 s/nest):
     FGDB query parser is slow on huge clauses (~50 chunks of 900 UIDs
     ANDed) and the optimization didn't pay back enough.
   Cursor-copy is in the seconds per nest.
4. **Erase.** Erase the buffer footprint from each *local* habitat subset
   (HIGH, MOD, and the derived LOW) so habitat that overlaps the buffer
   is not double-counted.
5. **Acres on work layers (planar).** `CalculateGeometryAttributes` with
   `method="AREA"` rather than `AREA_GEODESIC`. Geodesic on the work
   layers was the dominant per-nest cost (60%+ of setup time). The
   difference at Tahoe latitudes is <0.5%, and we only use the work-layer
   acres for sliver filtering and a running pre-fill total. Output
   numbers (`BufAc`, `TotalAc`) stay geodesic — those go to the user.
6. **Tiered growth with probe-based stop** (the core algorithm).
   Distance to the nest is computed once per habitat subset with
   `arcpy.analysis.Near` before the loop starts; ordering uses
   `NEAR_DIST` ascending (ties broken by larger `Acres`). Per-iteration
   shared-boundary scoring was tried but is too slow at TRPA scale.
   Candidates under `min_acres` (default 2.0 ac) are skipped to keep
   sub-2-ac slivers — which the close pass tends to eliminate — out
   of the running sum.

   **Phase 1 — strict contiguity** (`SelectLayerByLocation` relation =
   `INTERSECT`, `contiguity_relation` tunable). Three tiers:
   1. **Drain HIGH.** Repeatedly add the HIGH polygon closest to the
      nest that touches the current footprint. After each add, append
      the picked polygon's `SHAPE@` to the in-memory footprint via
      `InsertCursor` — no Merge/Dissolve per iteration.
   2. **One MOD step.** If no HIGH touches, add the single MOD polygon
      closest to the nest, then return to draining HIGH (the new
      footprint may now touch HIGH that didn't reach before).
   3. **One LOW step.** If no MOD touches either, add the single LOW
      polygon closest to the nest, then return to HIGH+MOD.
   Stop when projected `TotalAc ≥ target` or all three tiers have no
   contiguous candidate left.

   **Phase 2 — sparse-habitat fallback** (only if Phase 1 ended short).
   Re-run the same three-tier loop with the spatial-relation check
   dropped entirely: pick the unselected polygon with smallest
   `NEAR_DIST` to the nest from the in-memory layer (already pre-clipped
   to `search_radius`). Output may be multi-part for nests in fragmented
   habitat. The stagnation tracker is reset between phases.

   `WITHIN_A_DISTANCE` was tried as the relaxed contiguity check and
   abandoned — `SelectLayerByLocation` against a 50K-poly layer with a
   growing `current_fc` was orders of magnitude slower than just
   sorting by the pre-computed `NEAR_DIST` field.

   **Stop condition is post-fill, evaluated by periodic probe.** Every
   `probe_every` picks (default 5), and every pick once the pre-fill
   running sum is within `probe_homestretch_ac` of target (default 50),
   the algorithm runs the full final pipeline (Dissolve + interior hole-
   fill + morphological close + re-fill) on a copy of the current
   footprint and measures its geodesic acres. Growth stops when this
   projected `TotalAc` reaches `target_total_acres`. This replaces the
   prior pre-fill stop, which systematically overshot by 30–100+ ac
   because hole-fill area was not accounted for.

7. **Finalize geometry.** Once growth ends, run the same pipeline that
   the probe used:
   1. `Dissolve` the appended footprint once.
   2. `EliminatePolygonPart` (`hole_area_threshold = 10**18`) to remove
      all interior holes.
   3. **Morphological close** (`close_dist = "200 Feet"` default):
      `Buffer` outward then `Buffer` inward by the same distance. This
      collapses concavities, narrow channels, and "non-hole" gaps
      narrower than `2 * close_dist` (~400 ft at the default) into the
      polygon. These greedy-growth artifacts are not technically holes
      so `EliminatePolygonPart` does not catch them.
   4. Re-run `EliminatePolygonPart` to fill any interior holes the close
      may have newly enclosed.

   The reported `TotalAc` matches the value that triggered the stop
   because both come from the same pipeline.

8. **Output row.** Write one feature with attributes `NestOID`, `BufAc`,
   `HighAc`, `ModAc`, `LowAc`, `FilledAc`, `TotalAc` to
   `TRPA_Goshawk_Threshold_Zone` in the dedicated output GDB.

## Key assumptions

- **"Suitable habitat" tiers.** HIGH and MOD come from the CWHR habitat
  model on the ecobject layer. LOW is derived per-nest as
  `Vegetation_Ecobject_2010` minus HIGH minus MOD — i.e. any vegetated
  polygon the CWHR model didn't rate HIGH or MOD for goshawk. HIGH is
  preferred over MOD which is preferred over LOW. MOD is used only when
  no contiguous HIGH remains; LOW only when neither HIGH nor MOD has a
  contiguous candidate (last-resort fallback for sparse-habitat nests).
- **Contiguity, with a fallback.** Phase 1 requires picked polygons to
  touch the current footprint (`INTERSECT`). Phase 2 (only fires if
  Phase 1 ended short) drops the contiguity check and picks the
  unselected polygons with smallest `NEAR_DIST` to the nest from
  whatever's in the 5-mile pre-clip envelope. Output is multi-part for
  nests that go through Phase 2.
- **"Closest" candidate = smallest `NEAR_DIST` to the nest**, ties broken
  by larger Acres. Distance is computed once per layer with
  `arcpy.analysis.Near` before the growth loop begins. This is
  nest-relative rather than footprint-relative; a footprint-relative
  shared-boundary metric was tried but is too slow at TRPA habitat scale
  (per-iteration polyline intersection on a growing footprint).
- **Hole-fill removes ALL interior gaps** regardless of size. Threshold
  zones are reported as solid polygons.
- **Stop condition is post-fill projected `TotalAc`**, evaluated by a
  periodic probe that runs the full final pipeline (Dissolve + hole-fill
  + morphological close + re-fill) on a copy of the current footprint.
  Growth stops when projected `TotalAc ≥ target_total_acres`. Replaces
  the earlier pre-fill stop, which systematically overshot the target by
  30–100+ ac because the hole-fill area was unaccounted for at the stop
  decision.
- **Morphological close** (`close_dist = "200 Feet"` default) at the end
  of growth collapses concavities and narrow channels into the polygon.
  Closes any opening narrower than `2 * close_dist` (~400 ft at default).
  Tunable in the user-inputs block; raising it bridges wider gaps but
  risks fabricating area across legitimate non-habitat (roads, riparian
  corridors).
- **Sliver filter:** habitat polygons under `min_acres = 2.0` ac are
  not eligible for selection. Bumped from 0.5 because most ghost picks
  (those eliminated by the close pass) were sub-2-ac slivers; raising
  the floor here is much cheaper than tightening contiguity.
- **Search radius pre-filter.** Habitat layers are pre-clipped per nest
  to `search_radius = "5 Miles"` before `Erase`. Any HIGH/MOD/LOW
  polygon beyond 5 mi from a nest is not eligible — in practice the
  500-ac target is reached well within this radius. If a nest still
  warns after Phase 2 (sparse fallback), raising `search_radius` to
  e.g. `"7 Miles"` is the next thing to try.
- **Planar vs geodesic acres.** Work-layer acres (used for sliver
  filtering and the homestretch trigger for the probe) are planar.
  Output `BufAc`, `TotalAc`, and the probe's projected total are
  geodesic. The two can differ by ~0.5% at Tahoe latitudes — the math
  `BufAc + HighAc + ModAc + LowAc + FilledAc ≈ TotalAc` is therefore
  approximate to that tolerance, not exact.

## Output schema (`TRPA_Goshawk_Threshold_Zone`)

| Field | Type | Meaning |
|---|---|---|
| `NestOID` | LONG | OID of the source nest in `nogo_nest_usfs` |
| `BufAc` | DOUBLE | Geodesic acres in the 0.25-mile buffer |
| `HighAc` | DOUBLE | Planar acres of HIGH habitat added (post-erase, pre-finalize) |
| `ModAc` | DOUBLE | Planar acres of MOD habitat added (post-erase, pre-finalize) |
| `LowAc` | DOUBLE | Planar acres of LOW habitat added (post-erase, pre-finalize). Non-zero only when HIGH+MOD ran out before reaching target. |
| `FilledAc` | DOUBLE | Geodesic acres added by interior hole-fill **and** the morphological close combined. Catch-all for `TotalAc − (BufAc + HighAc + ModAc + LowAc)`. |
| `TotalAc` | DOUBLE | Geodesic acres of the final polygon (after dissolve + hole-fill + close + re-fill). Equals `BufAc + HighAc + ModAc + LowAc + FilledAc` to ~0.5% (planar/geodesic slack on the work-layer acres). |

## Known caveats

- **Unreachable nests.** A nest where Phase 1 (strict contiguity) AND
  Phase 2 (`NEAR_DIST` fallback over the 5-mile envelope) together
  still can't reach 500 ac produces a buffer-plus-whatever-was-
  reachable polygon and a `WARNING: Could not reach target acres
  contiguously` log line. The end-of-run summary lists these OIDs.
- **Greedy growth is not optimal.** The algorithm picks the locally best
  next polygon each step. This produces compact, defensible shapes but is
  not guaranteed to land exactly on 500 ac. The probe-based stop reduces
  overshoot considerably (typical residual is the size of the last pick,
  ~5–25 ac) but does not fully eliminate it. For TRPA review purposes
  this is preferred — biologically defensible compactness over numerical
  optimality.
- **Close-distance trade-off.** A 200-ft morphological close fills
  concavities and channels narrower than ~400 ft. This can bridge
  legitimate gaps if the local habitat configuration includes roads,
  rivers, or clearcuts narrower than that. If a reviewer flags a
  threshold zone for spanning a non-habitat feature, lower `close_dist`
  for that nest (or globally) and re-run.
- **Erased habitat slivers.** When a habitat polygon partially overlaps the
  buffer, `Erase` clips it to the area outside. Only that outside-the-buffer
  remainder is eligible to be added. The original overlap is already
  counted in `BufAc`.
- **Negative buffer requires projected SR.** The morphological close uses
  `arcpy.analysis.Buffer(..., "-200 Feet")` on the inward step. The
  source habitat layer's spatial reference (reused for output) must be
  projected for this to behave correctly. TRPA basin layers typically
  are. If swapped to a geographic SR, the close step would behave
  unpredictably.
- **In-memory growth.** Per-iteration footprints, probe outputs, and
  finalize intermediates are all kept in `in_memory` workspaces and
  cleaned up by an `arcpy_temp_manager` context manager. For scale >
  a few hundred nests this could pressure RAM.

## Run scope and performance

Runs over 136 goshawk nests in `nogo_nest_usfs`. No parallelism. Per-nest
timing is logged at each phase (buffer, pre-clip, erase, acres, near,
each pick, each probe, finalize) so a slow nest is visible in the log
rather than appearing hung. Progress is logged every 25 HIGH picks
within a nest, on every MOD or LOW pick, and on every probe.

The three optimizations that took per-nest runtime from "hung after 5+
minutes" to "tractable" are:

1. **Pre-clip** habitat to a 5-mile envelope before `Erase`
   (`SelectLayerByLocation` → `CopyFeatures`). Largest single win — the
   full basin HIGH/MOD layers are too big to `Erase` in a per-nest loop.
2. **Append, don't dissolve.** During growth, picked polygons are
   `InsertCursor`-ed directly into the footprint FC. The previous
   per-iteration Merge+Dissolve was O(n_picks²) on the footprint geometry
   and is the second largest bottleneck at TRPA scale. One final
   `Dissolve` runs after the loop.
3. **Planar acres on work layers.** `AREA` rather than `AREA_GEODESIC`
   for work-layer area — geodesic was ~60% of per-nest setup time.
   Reported output acres remain geodesic.

If runtime is still a problem, drop `search_radius` to `"3 Miles"` in the
user-inputs block. That roughly halves the post-filter polygon count for
most nests with negligible impact on the result (any nest that needs
habitat farther than 5 mi is already going to be flagged short).

### Test mode

Both [data_engineering.ipynb](data_engineering.ipynb) and
[goshawk_threshold_zone.py](goshawk_threshold_zone.py) carry the same
test-mode toggles at the top of `main()`:

- `TEST_OIDS = [...]` — run only those specific nest OIDs. Output FC
  gets a `_TEST_<TEST_LABEL>` suffix (e.g. `_TEST_FIXES`).
- `TEST_N = N` — run the first N nests. Output FC gets `_TEST_N{N}`.
- Both `None` — full 136-nest production run, output is
  `TRPA_Goshawk_Threshold_Zone` (no suffix).

The output FC suffixing means a test run can never clobber the
production output. After verifying a change, set both toggles back to
`None` for production.

## Downstream consumers

- TRPA threshold-evaluation reporting (Wildlife indicator).
- Project-review screening: any project intersecting a threshold zone
  triggers wildlife review.

## Changelog

### 2026-04-28
- Added LOW-suitability tier as a third fallback after HIGH and MOD are
  exhausted. There is no precomputed `_low` feature class — LOW is
  derived per-nest as `Vegetation_Ecobject_2010` (the source veg layer
  in `C:\GIS\TahoeMaps\Tahoe_Data.gdb`) minus HIGH minus MOD, after
  pre-clipping the source to a 5-mile envelope around the nest. The
  `veg_src` arg replaces an earlier `hab_low` arg that pointed at a
  precomputed FC that didn't actually exist.
- Tried tightening contiguity from `INTERSECT` to
  `SHARE_A_LINE_SEGMENT_WITH` to drop point-touch ghost picks; reverted
  because `SHARE_A_LINE_SEGMENT_WITH` was 3–4× slower per
  `SelectLayerByLocation` call than `INTERSECT` on TRPA's data sizes
  (e.g., OID 86: 167 s → 661 s with no change in result). Replaced
  with two cheaper interventions:
  - **Bumped `min_acres` from 0.5 to 2.0.** Most ghost picks were
    sub-2-ac slivers; raising the floor on candidate size eliminates
    them at the candidate-filter stage rather than via expensive
    spatial relations.
  - **Stagnation guard on probe cadence** (always on). When the last
    `stagnation_window` (default 5) probes all sit within
    `stagnation_tol` ac (default 0.5) of each other, probe cadence
    backs off to once every `stagnation_interval` picks (default 10).
    Caps the wasted-time cost when ghost picks do happen.
- Added a **sparse-habitat fallback pass.** After the strict-contiguity
  growth loop ends, if `projected_total < target_total_acres`, a second
  `grow_phase("fallback")` runs with the spatial-relation check dropped
  entirely: pick the unselected polygon with smallest `NEAR_DIST` from
  whatever's in the 5-mile pre-clip envelope. The earlier
  `WITHIN_A_DISTANCE` design was tried (2026-04-28 OID 6 stalled with
  +17 ac in 12 minutes of fallback) and abandoned —
  `SelectLayerByLocation` against a 50K-poly layer with a growing
  `current_fc` is the same O(features × current_fc) cost trap that
  killed `SHARE_A_LINE_SEGMENT_WITH`. Sorting by pre-computed
  `NEAR_DIST` is constant time per pick. Output may be multi-part for
  these nests.
- Moved output FC from `C:\GIS\Scratch.gdb` to a dedicated
  `C:\GIS\Goshawk_Threshold_Zone.gdb` (auto-created on first run). Keeps
  Scratch.gdb clean of per-run artifacts. `arcpy.env.workspace` still
  points at Scratch.gdb so input feature classes resolve by short name.
- Added explicit `clear_in_memory()` (wraps `arcpy.management.Delete(
  "in_memory")`) at the start and end of `main()` as a belt-and-
  suspenders cleanup. The per-nest `arcpy_temp_manager` still tracks and
  deletes individual temps; this just guarantees nothing leaks across
  aborted runs in the same Python kernel.
- Replaced the two-`Erase` LOW derivation with a Python cursor copy:
  `SearchCursor` over `veg_local` → `InsertCursor` into an empty target
  FC, skipping rows whose `UniqueID` is in the HIGH+MOD exclude set.
  Observed on nest OID 1: original two-`Erase` ~287 s; chunked SQL
  `NOT IN` ~188 s; cursor-copy is in the seconds. Equivalent result
  because HIGH and MOD are exact attribute selections from the same
  source veg layer (they share `UniqueID`).
- Added `TEST_OIDS` / `TEST_N` toggles in `main()` (both .py and the
  notebook). Test runs route to a `_TEST_<LABEL>` / `_TEST_N{N}`
  suffixed FC so production output is never clobbered.
- Replaced the pre-fill stop condition with a periodic post-fill probe.
  The probe runs the full final pipeline on a copy of the current
  footprint and measures geodesic acres; growth stops when the projected
  total reaches target. Overshoot drops from a typical 30–100+ ac to the
  size of the last pick. The pre-fill running sum is kept only as the
  cheap "are we in the homestretch" trigger for probe cadence.
- Added a 200-ft morphological close (outward `Buffer` then inward
  `Buffer`) at the end of growth, followed by a re-run of
  `EliminatePolygonPart`. Closes concavities and channels narrower than
  ~400 ft that the prior `EliminatePolygonPart` couldn't reach because
  they aren't fully-enclosed holes.
- Output schema: added `LowAc DOUBLE` between `ModAc` and `FilledAc`.
  `FilledAc` is now the catch-all for hole-fill plus close-added area.
- Changed the "short" detection in `main()` from `(high_added +
  mod_added) < (target - buf_ac - 0.01)` to `total_ac < (target - 0.01)`
  so it reports against the actual reported `TotalAc` rather than the
  pre-fill sum.

### 2026-04-27
- Initial methods documentation.
- Algorithm correctness pass: tiered HIGH→MOD priority (HIGH fully drained
  before any MOD; HIGH re-checked after each MOD add), `FilledAc`
  accounting, no-self-dissolve in footprint rebuild, `min_acres` sliver
  filter.
- Removed the older procedural cell from the notebook.
- Promoted the notebook to `goshawk_threshold_zone.py` with stdlib logging.
- Reverted ordering to one-time `arcpy.analysis.Near` after a per-iteration
  shared-boundary metric proved too slow at TRPA scale (script hung inside
  nest 1 of 136 on first run). Added per-pick progress logging.
- Performance pass after the first end-to-end run hung on setup of nest 1:
  added 5-mile per-nest pre-clip of HIGH/MOD habitat, replaced
  per-iteration Merge+Dissolve with InsertCursor append + single final
  Dissolve, switched work-layer acres to planar (`method="AREA"`),
  switched `SelectLayerByLocation` to keyword args to satisfy the modern
  Pro signature.
