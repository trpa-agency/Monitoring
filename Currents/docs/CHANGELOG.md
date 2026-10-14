# Changelog

## 2026-10-13
- Depth shading underlay from TRPA bathymetry (`build_bathymetry.py`, `data/bathy.json`), with a panel toggle.
- Page: mapped.earth style streak texture (dense short-lived particles, fast fade, glow plus core strokes, cyan-white ramp); playback as 1x to 8x multipliers; About dialog explaining the TERC model.

## 2026-10-11
- Initial daily pipeline: TERC S3 `.npy` hours and TVL winds merged into a 21 day rolling archive, deployed with the page to GitHub Pages by `currents-daily.yml` (Pages source: GitHub Actions, app at /Monitoring/Currents/).
