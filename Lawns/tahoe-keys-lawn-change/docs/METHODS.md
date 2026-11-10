# Methods: Tahoe Keys lawn change

**Owner:** mason.bindl@trpa.gov
**Last reviewed:** 2026-09-17

## Purpose

Quantify irrigated lawn area on every Tahoe Keys parcel from summer aerial
imagery and describe what happened to that lawn across the 2021 and 2022
irrigation shutoff: which parcels kept a lawn, which lost it and later
re-established it, and which lost it and stayed converted or bare. The
outputs support conversations with TKPOA, the water purveyor, and Lahontan
about turf, irrigation, and nutrient loading to the lagoons, and they give
a defensible baseline for any future turf conversion program.

## Why this design

Sentinel-2 at 10 m was ruled out: Keys lots are 0.15 to 0.2 acres, so a
single pixel mixes roof, driveway, canopy, and turf and nothing can be
attributed to a parcel. Sub-metre 4-band imagery is the right scale. NAIP
was chosen over commercial imagery for the first pass because it is free,
4-band, 0.6 m, flown in summer, and available for 2016, 2018, 2020, and
2022 over the Keys. The design is built so that a Nearmap or Hexagon year
can be dropped in without changing the method (see the Nearmap section).

Three decisions carry most of the weight:

1. **Classify each year on its own, then compare areas.** NAIP years are
   flown by different cameras with no shared calibration, and the imagery
   is 8-bit, not reflectance. Differencing NDVI across years would measure
   the camera as much as the lawn. Each year gets its own threshold (or its
   own trained classifier) and only the resulting areas are compared.
2. **Lidar decides what is low.** Tree canopy and shrubs are green in every
   year and would dominate a spectral-only count. The 2022 lidar nDSM masks
   everything at or above 0.5 m, buffered outward 1 m to swallow drip-line
   shadow and any lidar-to-imagery offset. The 2022 mask is applied to
   every year; Keys trees are mature and change slowly relative to lawns.
3. **The lawn footprint is fixed from before the shutoff.** Rather than
   asking "how much lawn is there in 2022", the pixel-level analysis asks
   "of the lawn that was there in 2020 (and in an earlier pre-year), which
   pixels are still green in 2022, and which are green again in 2024". This
   is what makes the 2020 to 2022 pair such a clean signal: irrigated turf
   browned out, canopy did not.

## Data sources

| Source | Type | Years | Notes |
|---|---|---|---|
| TRPA Zoning MapServer layer 0 | ArcGIS REST | current | `ZONING_ID` 102, 102_SA1, 102_SA2 dissolved is the study boundary |
| TRPA Parcels FeatureServer layer 0 | ArcGIS REST | current | Parcels with those zoning IDs; 1,450 parcels, 1,365 single family |
| NAIP 4-band 0.6 m, California | Planetary Computer STAC (`naip`) | 2016-07-12, 2018-09-17, 2020-07-31 (8% 07-24), 2022-07-21 | Keys sit in quads 3812008 SE and NE; the 3811901 quads (flown on other dates) only touch a sliver on the east edge and are given lower priority in the mosaic |
| NAIP 2024 and later | Local GeoTIFF via `config.yaml` | as available | Not on Planetary Computer or the USGS NAIP image service at build time |
| 2022 lidar DSM and DTM | Local rasters | 2022 | nDSM = DSM minus DTM, warped to the common grid |
| Building footprints | Local polygons (optional) | | If absent, a rough mask is derived from the nDSM (tall, flat, not green) |

The 2018 Hexagon 4-band imagery on maps.trpa.org is a JPEG-compressed
tile cache and is not used for analysis. If the source GeoTIFFs are
located they can be added as a local year the same way as 2024.

## Processing steps

### 01_extract

1. Boundary from the zoning service, dissolved, reprojected to EPSG:26910.
2. Parcels selected on the same zoning IDs. APN kept as string. Split
   parcels (repeated APN) are dissolved to one geometry per APN.
3. A common grid is defined once: 0.6 m pixels, origin snapped to a
   multiple of the pixel size, padded 30 m around the boundary. Every
   raster in the project is warped onto this grid, so arrays can be
   compared with plain numpy.
4. Parcel polygons are inset 0.5 m and rasterized to a parcel ID raster.
   The inset keeps fence lines and road shoulders out of the count.
5. NAIP: for each year, quarter quads intersecting the boundary are read
   straight from the cloud-optimized GeoTIFFs through a `WarpedVRT` onto
   the grid and mosaicked, first valid pixel wins. Quads are ordered by
   how much of the Keys they cover, California over Nevada on ties. A
   per-pixel flight-date raster is written alongside so a parcel can be
   tagged with the date it was actually imaged.
6. nDSM from the lidar, canopy mask at 0.5 m buffered 1 m, building mask
   from footprints or the nDSM fallback.

### 02_classify

1. Water mask, built once from the 2020 image: NDWI > 0.15 and NIR < 90,
   opened, then only connected components of at least 2,000 m² are kept
   (dark roofs pass the spectral test but are small islands; the lagoons
   are one body), then dilated 2 pixels. Applied to every year. The
   lagoons sit inside common-area parcels, so this matters: about 42% of
   parcel pixels are water.
2. Per year: NDVI and brightness from the 8-bit bands. Eligible pixels are
   inside a parcel, valid, not canopy, not building, not water, and not
   deep shadow (mean RGB below 40). Eligible area is the denominator for
   all percentages, so a heavily treed lot is not penalised for canopy.
3. Threshold: Otsu on the NDVI of eligible pixels, per year, unless
   `analysis.ndvi_threshold` fixes one, or a training layer
   `data/raw/training/training_<year>.gpkg` exists (polygons with a
   `class` field of `lawn` / `not_lawn`), in which case a random forest on
   R, G, B, NIR, NDVI, GNDVI, brightness, and 3x3 NDVI texture is used.
   The Otsu thresholds on the test run landed between 0.09 and 0.13 for
   all four years, which is consistent with the imagery being 8-bit DN
   rather than reflectance.
4. Speckle: 3x3 majority filter, then connected components under
   5 m² dropped.
5. Tabulation is `np.bincount` on the parcel ID raster for lawn,
   eligible, canopy, building, water, and shadow pixels, times pixel
   area. Outputs a long table (APN, year, acq_date, areas, percentages)
   and a per-year CSV.

### 03_change

1. Lawn footprint = lawn in the last pre-shutoff year (2020) and in at
   least one earlier pre-year.
2. Pixel trajectory raster: footprint pixels still lawn in every
   during-year image are "kept"; the rest "died". With a post year, died
   pixels that are lawn again are "recovered", the rest "stayed"; lawn
   outside the footprint in a post year is "new".
3. Parcel trajectory class from window areas (mean of pre years, last pre
   year, mean of during years, mean of post years), with rules from
   `config.yaml`:
   - had lawn = last pre year lawn at least 20 m²
   - a loss counts when it is at least 30% of last-pre lawn and at least
     20 m² absolute
   - recovered = post lawn at least 70% of last-pre lawn
   Classes: kept lawn through shutoff; lost during shutoff, recovered
   after; lost during shutoff, stayed converted or bare; lost lawn during
   shutoff (post image pending); gained lawn after shutoff; no lawn before
   shutoff.
4. Only single family, multi-family, and vacant parcels are in the change
   summaries. Commercial, open space, and public service parcels are
   tabulated but reported separately because their turf patterns differ.
5. Validation sample: stratified random draw of 12 parcels per trajectory
   class, exported to `outputs/validation_sample.gpkg` for manual
   digitizing. When `data/raw/validation/reference_lawn.gpkg` exists
   (layers `lawn_<year>` with an APN field) the notebook reports
   per-parcel bias, RMSE, R², and area-weighted producer's and user's
   accuracy per year.

## Key assumptions

- Lawn means green, low, pervious surface in a summer image. It includes
  irrigated turf and any well-watered groundcover under 0.5 m. It excludes
  artificial turf, which is dark in NIR.
- The 2022 canopy mask is valid for 2016 through the most recent year.
- One flight date per year is representative of that summer. Dates: 2016
  July 12, 2018 September 17, 2020 July 31, 2022 July 21. Irrigated turf is
  green on all of these; unirrigated grass is greener in July than in
  September, so July years may slightly over-count marginal lawn.
- The 2022 image (second shutoff season) represents the "during" state. The
  2021 season is unobserved.

## Known caveats

- Without the lidar, "kept green" pixels include tree canopy and the
  parcel-level "kept lawn" class is inflated. The test run in this repo
  was executed without lidar and its numbers are for checking mechanics
  only.
- Converted-to-rock, converted-to-mulch, dead turf left in place, and
  bare soil are indistinguishable here. They all land in "stayed
  converted or bare". Separating them needs finer imagery or a field
  check.
- Shadow varies with sun angle and time of day between years. Deep shadow
  is excluded from both numerator and denominator, but partial shadow
  lowers NDVI and can push marginal lawn below the threshold. The
  validation sample is the check on this.
- NDVI thresholds from Otsu assume a bimodal histogram. If a future year's
  histogram is not bimodal (a very late-season image, or heavy smoke),
  fix the threshold in `config.yaml` from a training sample instead.
- Parcel polygons in the Keys include lagoon water on the common-area
  parcels. Water is masked, but the parcel area used in `lawn_pct_parcel`
  still includes it. Use `lawn_pct_eligible` for comparisons.

## What annual July Nearmap NIR would add

Nearmap offers 4-band vertical imagery over South Lake Tahoe, on the order
of one July capture per year for 2021 onward, at roughly 7 to 15 cm (the
NIR band may be delivered at a coarser GSD than the RGB; confirm with the
account rep). Against the NAIP series, it changes four things:

1. **Annual instead of biennial, and it fills 2021.** The first shutoff
   season is the one NAIP misses entirely. A July 2021 capture would show
   whether lawns browned in the first season or only after two. Annual
   2023, 2024, 2025, and 2026 captures would show the recovery curve rather
   than one post point, which is the difference between "some are
   watering again" and "recovery started in year N and plateaued at X%".
2. **Consistent timing.** Every capture in the same month removes the
   July-versus-September question and makes year-to-year area comparisons
   cleaner. Confirm actual capture dates for the Keys before buying; the
   value is in the consistency, and a capture that drifts to June in one
   year is worth less.
3. **Finer classes.** At 7 to 15 cm, rock and mulch conversions have
   visible texture and colour, dead turf is a flat tan, and artificial
   turf has crisp edges and uniform tone. A random forest with texture
   features, or Nearmap's own AI vegetation layers (which include a lawn
   grass class), can separate "converted" from "left dead" and flag
   artificial turf. Those are exactly the classes this analysis cannot
   resolve from NAIP.
4. **Tree canopy updates.** Nearmap's DSM product, if available for the
   Keys, would let the canopy mask be refreshed per year instead of frozen
   at 2022.

What Nearmap does not change: it is still not radiometrically calibrated,
so each year is still classified on its own and only areas are compared.
Never difference NDVI between a NAIP year and a Nearmap year. Resample
Nearmap to a common grid (30 cm is a sensible compromise: 4x the NAIP
pixel count, still well above the 0.6 m NAIP grid) and run it through the
same pipeline as a local year. Use the years where both sources exist
(2022, possibly 2024) to cross-calibrate the two series: if NAIP and
Nearmap disagree on total lawn area by a consistent ratio, that ratio
can be applied when presenting the combined series.

Practical order: first confirm Nearmap capture dates and NIR coverage
for the Keys for 2021 through 2026. If 2021 is there and the dates are
consistently July, the purchase is worth it for the 2021 season alone.

## Published precedent

This is a well-trodden method. Closest analogues:

- Quesnel, Ajami, and Marx (2019), *Environmental Research Letters*:
  parcel-level lawn greenness from biennial summer NAIP (2010 to 2016)
  matched to irrigation meter data in Redwood City across California's
  2012 to 2016 drought. Found greenness decoupling from irrigation and a
  110% greenness rebound after the drought. Same imagery, same scale,
  same question shape as this project.
- Lassiter (2022), *Urban Forestry and Urban Greening*: NAIP NDVI on
  private parcels in the East Bay MUD service area with a fixed-effects
  panel model to detect the effect of a lawn replacement rebate program.
  Shows NAIP is good enough to detect small parcel-level changes when the
  design controls for year effects.
- Miller, Roberts, et al. (2020), *Remote Sensing of Environment*:
  airborne imaging spectroscopy over Santa Barbara separating drought
  response of turfgrass from that of trees, which is the canopy problem
  this project solves with lidar.
- Green et al. (2024), *Journal of Urban Ecology*: field survey of
  109,000 Sacramento front yards for water-wise conversion. A reminder
  that the conversion-versus-dead distinction is usually settled on the
  ground.

## Downstream consumers

- TKPOA and water purveyor discussions on irrigation policy
- Lahontan nutrient loading conversations about the lagoons
- Any future turf conversion incentive baseline

## Changelog

See `CHANGELOG.md`.
