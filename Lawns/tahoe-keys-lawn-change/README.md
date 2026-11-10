# Tahoe Keys lawn change

Quantifies irrigated lawn area on every Tahoe Keys parcel from summer
4-band NAIP imagery (2016, 2018, 2020, 2022, plus any later year supplied
locally), uses the 2022 lidar to keep tree canopy out of the count, and
classifies each parcel's trajectory across the 2021 to 2022 irrigation
shutoff: kept its lawn, lost it and recovered, lost it and stayed converted,
or gained lawn. Outputs are per-parcel tables, classified rasters, a parcel
GeoPackage for mapping in Pro, and a validation sample for manual review.

## Run order

1. `notebooks/01_extract.ipynb`: boundary and parcels from TRPA REST, NAIP
   from Planetary Computer warped onto one common grid, nDSM and canopy
   mask from the lidar DSM/DTM.
2. `notebooks/02_classify.ipynb`: per-year lawn classification, speckle
   removal, tabulation of lawn area by parcel.
3. `notebooks/03_change.ipynb`: trajectory classification, summaries and
   figures, validation sample export, and accuracy assessment once
   reference polygons exist.

See `environment.md` for setup and `docs/METHODS.md` for the method,
assumptions, caveats, and what annual Nearmap NIR would add.
