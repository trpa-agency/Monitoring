# Changelog

## 2026-09-17
- Initial build. Three notebooks (extract, classify, change), config, src helpers, METHODS.md.
- Test-executed end to end in a cloud sandbox against live TRPA REST services and Planetary Computer NAIP for 2016, 2018, 2020, 2022, without lidar (placeholder nDSM). Outputs from that run are mechanics checks only.
- Water mask changed from a plain NDWI rule to NDWI plus a 2,000 m² connected-component filter after dark roofs were masked as water.
- NAIP mosaic now orders quads by coverage of the Keys and writes a per-pixel flight-date raster.
