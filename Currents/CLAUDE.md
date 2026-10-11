# Lake Tahoe Currents

Single-file TRPA dashboard (Calcite, Plotly.js, AG Grid, Open Sans) that animates surface currents from the UC Davis TERC lake model. Rebuilt from the Claude artifact "Lake Tahoe Currents" (Oct. 2026) with the TRPA brand and dashboard-stack skills.

## Files

- `index.html`: the whole app. Config block `CFG` at the top of the script.
- `assets/`: TRPA logos (color for the white header, white for the dark stage) and favicon, copied from the maps repo (the trpa-brand skill's PNGs are not valid PNG files and render with a black box).
- `data/meta.json`: grid shape, lake mask (run-length encoded), shoreline rings, place labels, per-hour stats, airport wind vectors, time list.
- `data/currents.bin`: gzip of packed int8 u, int8 v, uint8 temperature per lake cell per model hour (12,430 cells). The loader also accepts base64 `.txt` or raw bytes.
- `scripts/update_currents.py`: the daily archive update (TERC S3 `.npy` hours plus TVL winds, 21 day rolling window). `config.yaml` holds sources and run parameters, `src/io.py` the config loader and logger, `docs/METHODS.md` the full method, `docs/CHANGELOG.md` dated changes.
- `.github/workflows/currents-daily.yml` (repo root): runs the script daily and deploys the Currents folder to GitHub Pages without committing data.

## Data provenance

- Currents and temperature: TERC Forecasted Conditions page, 3D lake model, 2 hour steps, about 200 m cells. Archive saved Oct. 4, 2026, covering Sept. 20 to Oct. 6. Hours after Oct. 4 at 2 p.m. are forecast.
- Wind: Iowa Environmental Mesonet ASOS archive, Lake Tahoe Airport (TVL).
- Times are Pacific local. Model surface temperatures run about 9 F warmer than TERC nearshore sensors; use them for pattern only.
- `data/` is the committed seed and is refreshed by running the script locally; the daily CI run reads the live Pages copy instead and never commits. The seed also carries the grid definition (mask, shoreline, places) that the script requires.
- TERC publishes the model as `.npy` files in the public `lake-tahoe-conditions` S3 bucket (found by reading the Forecasted Conditions page script). Arrays run south to north and must be flipped; u is east and v is north, verified by lake-mean currents following the airport wind. A daily all-zero placeholder file at 07:00 is skipped.

## Run locally

Needs a static server because the page fetches `data/`. From the repo root:

    python -m http.server 8765 --directory Currents

or use the `currents` entry in `.claude/launch.json`. Refresh the archive with the ArcGIS Pro Python (plain `python` lacks numpy). PowerShell needs the `&` call operator:

    & "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Currents\scripts\update_currents.py

## Conventions

- Page shell follows the maps.trpa.gov "map tool" pattern: thin white header with the color logo, agency overline, tool name, back link to All maps and apps, Beta chip. The Explore stage holds only the lake and place labels (no logo, it is in the header); every legend element (mode chip, scale bar, wind, probe help) lives in the fixed 440 px side panel and the map takes the rest of the width. The Reel layout keeps its own title, legend, credits, and logo because it exists for export.
- Animation is tuned to read calm: 2 hours per second default, sparse particles (1 per 26 px of lake), 8 to 20 second lifetimes, single-hue deep-navy-to-white speed ramp with no hot accent, slow water faint and fast water solid.
- Trails live on a transparent canvas faded with destination-out every fourth frame (alpha 0.045). Fading every frame with a small alpha leaves a permanent speckle residue because 8-bit alpha stops decreasing once a times alpha is under 0.5. Do not "simplify" this back to a per-frame fade.

- No dual-axis charts. The three series are stacked small multiples on a shared time axis.
- Chart colors: blue for current, earth for wind, brick for temperature, forest for the probe trace. Validated with the dataviz palette script in light mode.
- URL hash carries time, window, color mode, layout, and probe so a link reopens the same moment.
