# Lake Tahoe Currents

Single-file TRPA dashboard (Calcite, Plotly.js, AG Grid, Open Sans) that animates surface currents from the UC Davis TERC lake model. Rebuilt from the Claude artifact "Lake Tahoe Currents" (Oct. 2026) with the TRPA brand and dashboard-stack skills.

## Files

- `index.html`: the whole app. Config block `CFG` at the top of the script.
- `data/meta.json`: grid shape, lake mask (run-length encoded), shoreline rings, place labels, per-hour stats, airport wind vectors, time list.
- `data/currents.bin`: gzip of packed int8 u, int8 v, uint8 temperature per lake cell per model hour (198 hours, 12,430 cells). The loader also accepts base64 `.txt` or raw bytes.

## Data provenance

- Currents and temperature: TERC Forecasted Conditions page, 3D lake model, 2 hour steps, about 200 m cells. Archive saved Oct. 4, 2026, covering Sept. 20 to Oct. 6. Hours after Oct. 4 at 2 p.m. are forecast.
- Wind: Iowa Environmental Mesonet ASOS archive, Lake Tahoe Airport (TVL).
- Times are Pacific local. Model surface temperatures run about 9 F warmer than TERC nearshore sensors; use them for pattern only.
- The scraper and packer that produced `data/` are not in this repo. To refresh, rebuild `meta.json` and `currents.bin` with the same layout.

## Run locally

Needs a static server because the page fetches `data/`. From the repo root:

    python -m http.server 8765 --directory Currents

or use the `currents` entry in `.claude/launch.json`.

## Conventions

- No dual-axis charts. The three series are stacked small multiples on a shared time axis.
- Chart colors: blue for current, earth for wind, brick for temperature, forest for the probe trace. Validated with the dataviz palette script in light mode.
- Particle ramps are multi-hue sequential, lightness rising from navy to white then warming to orange for the fastest water.
- URL hash carries time, window, color mode, layout, and probe so a link reopens the same moment.
