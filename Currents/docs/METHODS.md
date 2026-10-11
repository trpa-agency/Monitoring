# Methods: Lake Tahoe Currents archive

**Owner:** mbindl@trpa.gov
**Last reviewed:** 2026-10-11

## Purpose

Keeps a rolling archive of modeled Lake Tahoe surface currents and temperature, plus matching airport winds, in the compact format the Currents page animates. Runs daily so the page always shows the past three weeks and TERC's three-day forecast.

## Data sources

| Source | Type | Refresh | Notes |
|---|---|---|---|
| TERC lake model, `lake-tahoe-conditions` S3 bucket | `.npy` per model hour, `flow/` (2 x 174 x 102 float64, u and v in m/s) and `temperature/` (174 x 102 float64, degrees C) | Every two hours; bucket holds about two weeks back and three days ahead | Same files the TERC Forecasted Conditions page reads. Filenames are Pacific local time. Arrays run south to north; land is NaN. |
| Iowa Environmental Mesonet ASOS archive | CSV, hourly METAR for TVL (Lake Tahoe Airport) | Hourly | Knots and from-direction. |
| Seed archive in `data/` | `meta.json`, `currents.bin` | Committed once | Supplies the lake mask, shoreline rings, place labels, and grid constants. The daily run never changes those. |

## Processing steps (`scripts/update_currents.py`)

1. **Previous archive.** Read `meta.json` and `currents.bin` from `--prev` (the live site in CI, the local `data/` folder otherwise). Fall back to the committed seed if that fails.
2. **Manifest.** Read `contents.json` from the bucket. The latest hour not after the current Pacific time becomes `forecast_issued`.
3. **Hours to fetch.** Every manifest hour that is missing from the archive, plus every hour that was forecast in the previous archive (later than its `forecast_issued`), since TERC reruns those. Hindcast hours already stored are kept as first published.
4. **Orient and pack.** Flip each array vertically so row 0 is north, confirm the NaN mask equals the archive's lake mask, then pack u and v with signed square-root quantization to int8 at 0.5 m/s full scale, and temperature to uint8 in 0.1 degree C steps from 8 degrees C (255 is land). Per-hour stats: lake-mean temperature in F, mean, 99th percentile, and max speed, and the cell of the max.
5. **Rolling window.** Drop hours older than `rolling_days` before `forecast_issued`.
6. **Wind.** Pull TVL observations for the archive span. Convert to mph, flip from-direction to toward, split into east and north components, and average the observations within `wind_window_min` of each model hour. Calm or missing direction gives a zero vector; no observation gives null.
7. **Default window.** The windiest `event_days` span in the hindcast, by mean wind speed, becomes `default_start` and `default_end` (the page labels it "wind event").
8. **QA.** Warnings for irregular time steps, mask mismatches, out-of-range speed or temperature means, missing wind, and short archives. The run does not halt on QA; read the log.
9. **Write.** `meta.json` and gzip `currents.bin` to `--out`.

## Deployment

`.github/workflows/currents-daily.yml` runs at 14:30 UTC daily. It stages `index.html` and `assets/`, runs the script with the live site as `--prev` and `site/data` as `--out`, and deploys the folder to GitHub Pages. No commits, so the 4 MB archive never enters git history. Requires Pages source set to GitHub Actions on the repository.

## Key assumptions

- u is east-positive and v is north-positive in the TERC arrays. The TERC page only draws speed, so this was not verifiable from their code. `run.v_sign` in `config.yaml` flips v if lake-mean currents prove to run against the wind.
- The grid (174 rows by 102 columns of about 200 m, rotated 1.85 degrees, centered on 39.09363, -120.04734) has not changed since the seed. The mask check enforces this; a changed grid stops frames from being added until the seed is rebuilt.
- Model times are Pacific local time (TERC's page formats them in America/Los_Angeles).

## Known caveats

- TERC reruns past hours. Hindcast hours in the archive are the values as of the first run that covered them as hindcast, which may differ from what the TERC site shows later.
- Model surface temperature can run several degrees warmer than TERC's nearshore sensors. The page says to read temperature colors as pattern, not value.
- The bucket occasionally lags; a run with nothing new simply redeploys the same archive.

## Downstream consumers

- Currents page: `Currents/index.html`, published at the repository's GitHub Pages URL.

## Changelog

See `CHANGELOG.md`.
