# CLAUDE.md: Tahoe Keys lawn change

Project memory for Claude Code sessions in this repo. Read this first, then
`docs/METHODS.md` for the full method and `config.yaml` for every tunable.

## What this is

A TRPA analysis quantifying irrigated lawn area per parcel in the Tahoe
Keys (South Lake Tahoe) from summer 4-band aerial imagery, and classifying
what each parcel did across the 2021 and 2022 irrigation shutoff: kept its
lawn, lost it and recovered, lost it and stayed converted or bare, gained
lawn, or never had lawn. Owner: Mason Bindl, Senior GIS Analyst, TRPA.

The motivating question: anecdotally some Keys residents converted to
low-water landscaping during the shutoff, others let the lawn die and are
now watering it again. Which, and how much.

## Conventions (follow the trpa-data-engineering skill)

- Notebook-first, linear code, no class hierarchies. Three numbered
  notebooks in `notebooks/`, shared helpers only in `src/`.
- `config.yaml` holds every path, URL, threshold, and window. Never
  hardcode a threshold in a notebook.
- Standard library `logging` via `src/io.get_logger`; every run writes to
  `logs/`.
- APN is always a string.
- Analysis CRS is EPSG:26910. Everything rasterized sits on the one common
  grid in `data/processed/grid.json` (0.6 m, origin snapped). If you add a
  raster, warp it onto that grid with `src/grid.py` helpers; never compare
  arrays that are not on it.
- Environment: a clone of `arcgispro-py3` named `keys-lawn` with rasterio,
  pystac-client, planetary-computer, pyyaml, python-dotenv added. See
  `environment.md`. No arcpy is used.
- No em-dashes in prose. Oxford commas. Plain language.

## Method in one paragraph

Each image year is classified on its own (Otsu NDVI threshold on eligible
pixels by default, random forest when a training layer exists), because the
years are not radiometrically comparable. Lidar nDSM masks anything at or
above 0.5 m (buffered 1 m) so canopy never counts as lawn. Water and
buildings are masked. Lawn area is tabulated per parcel with `np.bincount`
on a parcel ID raster. The lawn footprint is fixed from 2020 (backed by an
earlier pre-year) and later years are asked whether each footprint pixel is
still green. Parcel trajectory classes come from pre / during / post window
areas with thresholds in `config.yaml` `change`.

## Data facts worth not rediscovering

- Study boundary: TRPA Zoning MapServer layer 0, `ZONING_ID IN
  ('102','102_SA1','102_SA2')`, dissolved. 495 acres.
- Parcels: TRPA Parcels FeatureServer layer 0, same zoning filter. 1,450
  parcels: 1,365 single family, 54 vacant, 12 open space, 6 commercial,
  5 multi-family, 1 public service, 7 blank land use. 1,137 in the base
  zone, 295 in Special Area 2, 18 in Special Area 1.
- The lagoons are inside common-area parcels; about 42% of parcel pixels
  are water. The water mask matters.
- NAIP on Planetary Computer covers the Keys for 2016 (July 12), 2018
  (September 17), 2020 (July 31, with 8% of parcels July 24), and 2022
  (July 21). The Keys fall within quads 3812008 SE and NE. 2024 California
  NAIP was not on Planetary Computer or the USGS NAIP image service as of
  2026-09-17; add it as a local year in `config.yaml` when it turns up.
- Otsu NDVI thresholds land around 0.09 to 0.13 on 8-bit NAIP DNs.
- The 2020 to 2022 pair shows the lawn die-off clearly. Canopy stays
  green, turf browns. That contrast is the core of the story.
- 2022 lidar DSM and DTM exist at TRPA; put them at
  `data/raw/lidar/2022_dsm.tif` and `2022_dtm.tif`. Without them the
  pipeline runs but "kept green" is contaminated by canopy.
- The 2018 Hexagon CIR on maps.trpa.org is a tile cache, not analysis-grade.
  Source GeoTIFFs would be a usable extra year.

## State as of 2026-09-17

- All three notebooks executed end to end in a cloud sandbox against live
  services, without lidar. Outputs in `outputs/` from that run are
  mechanics checks, not results.
- Not yet done: run with the real lidar; add 2024 NAIP; digitize training
  polygons if Otsu looks wrong on any year; digitize the validation
  sample; decide whether to buy Nearmap July NIR for 2021 onward (the 2021
  season is the gap that matters most).

## Next steps, in order

1. Drop in the lidar rasters, rerun 01 and 02, look at the class figure in
   02 and the trajectory figure in 03. Check that canopy is gone from
   "kept green".
2. Get 2024 NAIP (USDA GeoHub or EarthExplorer, quads 3812008 SE and NE),
   set `sources.naip.local.2024`, rerun 01 to 03. The post-dependent
   classes then populate.
3. Open `outputs/validation_sample.gpkg` in Pro, digitize lawn per year for
   those parcels into `data/raw/validation/reference_lawn.gpkg` (layers
   `lawn_<year>`, field `APN`), rerun the accuracy cell in 03.
4. If any year's NDVI histogram in 02 is not bimodal, digitize
   `data/raw/training/training_<year>.gpkg` (field `class`: lawn /
   not_lawn) and the notebook switches that year to random forest.
5. Ask Nearmap for Keys capture dates and NIR availability 2021 to 2026
   before deciding on a purchase. See METHODS.md.

## Things not to do

- Do not difference NDVI between years or between sources. Compare areas.
- Do not replace the Otsu / RF choice with a single fixed NDVI threshold
  across years without checking each year's histogram.
- Do not report `lawn_pct_parcel` for common-area parcels; use
  `lawn_pct_eligible`.
- Do not present numbers from a run whose 01 log says "LIDAR NOT FOUND".
