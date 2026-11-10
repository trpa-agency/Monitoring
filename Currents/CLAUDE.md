# Lake Tahoe Currents

Single-file TRPA dashboard (Calcite, Plotly.js, AG Grid, Open Sans) that animates surface currents from the UC Davis TERC lake model. Rebuilt from the Claude artifact "Lake Tahoe Currents" (Oct. 2026) with the TRPA brand and dashboard-stack skills.

## Files

- `index.html`: the whole app. Config block `CFG` at the top of the script.
- `assets/`: TRPA logos (color for the white header, white for the dark stage) and favicon, copied from the maps repo (the trpa-brand skill's PNGs are not valid PNG files and render with a black box).
- `data/meta.json`: grid shape, lake mask (run-length encoded), shoreline rings, place labels, per-hour stats, airport wind vectors, time list.
- `data/currents.bin`: gzip of packed int8 u, int8 v, uint8 temperature per lake cell per model hour (12,430 cells). The loader also accepts base64 `.txt` or raw bytes.
- `data/bathy.json`: one depth shade byte per lake cell (0 deepest, 255 shallowest) from `scripts/build_bathymetry.py`, a one-off that samples TRPA's rendered bathymetry layers onto the model grid. The page paints it under the flow lines as a dark luminance gradient, toggled by the Depth shading switch (`bathy=0` in the hash turns it off). The daily workflow copies it into the deploy.
- `scripts/update_currents.py`: the daily archive update (TERC S3 `.npy` hours plus TVL winds, 21 day rolling window). `config.yaml` holds sources and run parameters, `src/io.py` the config loader and logger, `docs/METHODS.md` the full method, `docs/CHANGELOG.md` dated changes.
- `.github/workflows/currents-daily.yml` (repo root): runs the script daily and deploys the Currents folder (page, assets, fresh data) to GitHub Pages at https://trpa-agency.github.io/Monitoring/Currents/ without committing data. Pages source is GitHub Actions; the site root redirects to Currents/ and is free for other Monitoring tools.

## Data provenance

- Currents and temperature: TERC Forecasted Conditions page, 3D lake model, 2 hour steps, about 200 m cells. The archive rolls daily (21 days); the About dialog and About tab read the dates from meta.json.
- Wind: Iowa Environmental Mesonet ASOS archive, Lake Tahoe Airport (TVL).
- Times are Pacific local. Model surface temperatures run about 9 F warmer than TERC nearshore sensors; use them for pattern only.
- `data/` is the committed seed and is refreshed by running the script locally; the daily CI run reads the live site's `data/` instead and never commits. The seed also carries the grid definition (mask, shoreline, places) that the script requires.
- TERC publishes the model as `.npy` files in the public `lake-tahoe-conditions` S3 bucket (found by reading the Forecasted Conditions page script). Arrays run south to north and must be flipped; u is east and v is north, verified by lake-mean currents following the airport wind. A daily all-zero placeholder file at 07:00 is skipped.

## Run locally

Needs a static server because the page fetches `data/`. From the repo root:

    python -m http.server 8765 --directory Currents

or use the `currents` entry in `.claude/launch.json`. Refresh the archive with the ArcGIS Pro Python (plain `python` lacks numpy). PowerShell needs the `&` call operator:

    & "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Currents\scripts\update_currents.py

## Conventions

- Page shell follows the maps.trpa.gov "map tool" pattern: thin white header with the color logo, agency overline, tool name, back link to All maps and apps, Beta chip. The Explore stage holds only the lake and place labels (no logo, it is in the header); every legend element (mode chip, scale bar, wind, probe help) lives in the fixed 440 px side panel and the map takes the rest of the width. The Reel layout keeps its own title, legend, credits, and logo because it exists for export.
- Playback is a multiplier of one model hour per second (1x, 2x default, 4x, 8x). Speed ramp was sampled from mapped.earth's wind streamlines by luminance band: slate navy, steel, sky, aqua, pale cyan white.
- Symbology follows mapped.earth's Lake Tahoe reel as far as the data allows: a dense field of particles (1 per 7 px of lake, capped at 22,000, living 6 to 16 s), each drawing one 0.5 px hairline segment per frame onto a trail canvas rendered at 2 device pixels per CSS px (so lines stay hairlines on any screen), erased by 4.5 percent alpha every frame (about 50 frames of trail). Coverage is density times trail length times alpha; push any of them and strokes pile up until the canvas saturates to white, which happened at 1 per 3 px with 110 frame trails. Judge density on a real screen: the in-app browser pane throttles the page, which trips the adaptive thinning and under-renders the field. Default color is temperature on a deliberately narrow 1.5 degree span (`CFG.tempSpanF`), chosen for drama over fidelity at Mason's request; the fastest speed bin draws wider and brighter (`fastWidthBoost`) so moving water pops. No glow pass; it fattened the lines and muddied color (the config still allows one). Temperature ramp: navy, royal, sky, teal middle, sand, orange, centered on the window median; brightness by speed. One stroke per (color bin, speed-alpha bin) per frame. A 5 percent per-frame fade is about the floor: 8-bit alpha stops converging below roughly 2 percent and leaves a permanent speckle residue. `window.__currentsBench()` in the console reports step and render cost in ms per frame; the loop thins the field automatically above about 14 ms.

- Phones (under 600 px): the map is the first screen (stage height is the viewport minus the header and a peek of the clock row), the header is one row with Share and PNG hidden, place labels sit inside the water edge with a dark halo so the lake uses the full width, the trail canvas renders at most 2 device px per CSS px, and the particle field is 70 percent of desktop density with a lower cap. Temperature ramp is a fixed 3 degree F span centered on the window median (`CFG.tempSpanF`). Reel on a phone (900 px and under) takes the whole screen (`body.reel-full`, stage fixed to the viewport) with a round Calcite close button back to Explore; Escape also closes it. Narrow reel frames (under 500 px) center the lake and draw labels inside the water.
- No dual-axis charts. The three series are stacked small multiples on a shared time axis.
- Chart colors: blue for current, earth for wind, brick for temperature, forest for the probe trace. Validated with the dataviz palette script in light mode.
- URL hash carries time, window, color mode, layout, and probe so a link reopens the same moment.
