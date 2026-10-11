"""
Lake Tahoe Currents: daily archive update.

Pulls the latest UC Davis TERC lake model hours (surface flow and temperature)
and Lake Tahoe Airport winds, merges them into the rolling archive the page
reads (data/meta.json and data/currents.bin), and writes QA to the log.

Run locally (writes into Currents/data):
    python scripts/update_currents.py

Run in CI with the live archive as the starting point and a build directory as output:
    python scripts/update_currents.py --prev https://trpa-agency.github.io/Monitoring/data --out site/data

Linear, top to bottom. See docs/METHODS.md for the why.
"""
import argparse
import gzip
import io
import json
import math
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.io import get_logger, load_config, project_root  # noqa: E402

PACIFIC = ZoneInfo("America/Los_Angeles")
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "TRPA-currents-archive/1.0 (gis@trpa.gov)"


# ── small helpers ──────────────────────────────────────────────────────────────
def tstr(dt: datetime) -> str:
    """Archive time key: local Pacific wall clock, minute precision, no zone."""
    return dt.strftime("%Y-%m-%dT%H:%M")


def tparse(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=PACIFIC)


def fetch(url: str, timeout: int = 60) -> bytes:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def pack_q(x: np.ndarray, vmax: float) -> np.ndarray:
    """Signed square-root quantization to int8: q = sign(x) * sqrt(|x| / vmax) * 127."""
    q = np.sign(x) * np.sqrt(np.clip(np.abs(x) / vmax, 0, 1)) * 127
    return np.round(np.nan_to_num(q)).astype(np.int8)


def unpack_q(q: np.ndarray, vmax: float) -> np.ndarray:
    f = q.astype(np.float64) / 127
    return np.sign(f) * f * f * vmax


def rle(idx: np.ndarray) -> list:
    """Run-length encode sorted cell indices as [start, length, start, length, ...]."""
    out = []
    start = prev = int(idx[0])
    for c in idx[1:]:
        c = int(c)
        if c != prev + 1:
            out += [start, prev - start + 1]
            start = c
        prev = c
    out += [start, prev - start + 1]
    return out


def unrle(r: list) -> np.ndarray:
    idx = []
    for i in range(0, len(r), 2):
        idx.extend(range(r[i], r[i] + r[i + 1]))
    return np.array(idx, dtype=np.int64)


# ── main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prev", help="directory or URL holding the previous meta.json and currents.bin")
    ap.add_argument("--out", help="output directory (default: data/ in the project)")
    ap.add_argument("--max-hours", type=int, default=None, help="cap on new hours to fetch (testing)")
    args = ap.parse_args()

    cfg = load_config()
    log = get_logger("update_currents")
    src = cfg["sources"]
    run = cfg["run"]
    out_dir = Path(args.out) if args.out else project_root() / cfg["paths"]["data"]
    out_dir.mkdir(parents=True, exist_ok=True)
    prev = args.prev or str(out_dir)
    log.info("=" * 60)
    log.info(f"{cfg['project']['name']}: prev={prev} out={out_dir} rolling_days={run['rolling_days']}")
    log.info("=" * 60)

    # ── 1. previous archive ──────────────────────────────────────────────────
    frames: dict[str, bytes] = {}   # time key -> packed bytes (u int8, v int8, t uint8 per lake cell)
    stats: dict[str, dict] = {}
    meta_prev = None
    try:
        if prev.startswith("http"):
            meta_prev = json.loads(fetch(prev.rstrip("/") + "/meta.json"))
            raw = fetch(prev.rstrip("/") + "/currents.bin")
        else:
            meta_prev = json.loads(Path(prev, "meta.json").read_text(encoding="utf-8"))
            raw = Path(prev, "currents.bin").read_bytes()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        n = meta_prev["ncell"] * 3
        for i, t in enumerate(meta_prev["times"]):
            frames[t] = raw[i * n:(i + 1) * n]
            stats[t] = meta_prev["stats"][i]
        log.info(f"Previous archive: {len(frames)} hours, {meta_prev['times'][0]} to {meta_prev['times'][-1]}, "
                 f"forecast issued {meta_prev['forecast_issued']}")
    except Exception as e:  # first run or unreachable site: start from the committed seed
        seed = project_root() / cfg["paths"]["data"]
        if prev != str(seed) and Path(seed, "meta.json").exists():
            log.warning(f"Could not read previous archive from {prev} ({e}); falling back to seed {seed}")
            meta_prev = json.loads(Path(seed, "meta.json").read_text(encoding="utf-8"))
            raw = gzip.decompress(Path(seed, "currents.bin").read_bytes())
            n = meta_prev["ncell"] * 3
            for i, t in enumerate(meta_prev["times"]):
                frames[t] = raw[i * n:(i + 1) * n]
                stats[t] = meta_prev["stats"][i]
        else:
            log.error(f"No previous archive available ({e}). The grid definition (mask, shoreline, places) "
                      "comes from the seed archive, so a seed is required.")
            raise

    rows, cols = meta_prev["rows"], meta_prev["cols"]
    idx = unrle(meta_prev["mask_idx_rle"])
    ncell = len(idx)
    vmax = meta_prev["vel_max_ms"]
    tmin_c, tstep = meta_prev["temp_min_c"], meta_prev["temp_step_c"]
    old_issued = meta_prev["forecast_issued"]

    # ── 2. TERC manifest ─────────────────────────────────────────────────────
    base = src["terc_s3"].rstrip("/")
    manifest = json.loads(fetch(f"{base}/contents.json"))
    available = {}
    for name in manifest.get("flow", []):
        if name in manifest.get("temperature", []):
            try:
                dt = datetime.strptime(name, "%Y-%m-%d %H.npy").replace(tzinfo=PACIFIC)
            except ValueError:
                continue
            available[tstr(dt)] = name
    now = datetime.now(PACIFIC)
    past = [t for t in available if tparse(t) <= now]
    new_issued = max(past) if past else old_issued
    log.info(f"TERC manifest: {len(available)} hours {min(available)} to {max(available)}; "
             f"now {tstr(now)}; forecast issued {new_issued}")

    # Fetch hours that are new, or were forecast in the previous archive and have since been re-run.
    # Hindcast hours already stored stay as published at the time.
    known_placeholders = set(meta_prev.get("placeholders", []))
    want = sorted(t for t in available
                  if (t not in frames or t > old_issued) and not (t in known_placeholders and t <= old_issued))
    if args.max_hours:
        want = want[-args.max_hours:]
    log.info(f"Hours to fetch: {len(want)} ({want[0] if want else '-'} to {want[-1] if want else '-'})")

    # ── 3. fetch, orient, pack ───────────────────────────────────────────────
    mask_full = np.zeros(rows * cols, bool)
    mask_full[idx] = True
    v_sign = float(run.get("v_sign", 1))
    fetched = 0
    skipped_zero = sorted(t for t in known_placeholders if t in available)
    for t in want:
        name = urllib.parse.quote(available[t])
        try:
            flow = np.load(io.BytesIO(fetch(f"{base}/flow/{name}")))
            temp = np.load(io.BytesIO(fetch(f"{base}/temperature/{name}")))
        except Exception as e:
            log.warning(f"{t}: download failed ({e}); skipped")
            continue
        if flow.shape != (2, rows, cols) or temp.shape != (rows, cols):
            log.warning(f"{t}: unexpected shapes flow {flow.shape} temp {temp.shape}; skipped")
            continue
        # TERC arrays run south to north; the archive runs north to south (row 0 at the top of the map).
        u = flow[0][::-1].reshape(-1)
        v = v_sign * flow[1][::-1].reshape(-1)
        tc = temp[::-1].reshape(-1)
        valid = ~np.isnan(u)
        if not np.array_equal(valid, mask_full):
            diff = int((valid != mask_full).sum())
            log.warning(f"{t}: lake mask differs from the archive in {diff} cells; frame skipped")
            continue
        uu, vv, tt = u[idx], v[idx], tc[idx]
        if not np.any(uu) and not np.any(vv):
            # the bucket carries an all-zero placeholder file some hours (seen daily at 07:00); not a model result
            log.info(f"{t}: all-zero flow field in the bucket; skipped as a placeholder")
            if t not in skipped_zero:
                skipped_zero.append(t)
            continue
        tq = np.where(np.isnan(tt), 255, np.clip(np.round((tt - tmin_c) / tstep), 0, 254)).astype(np.uint8)
        frames[t] = pack_q(uu, vmax).tobytes() + pack_q(vv, vmax).tobytes() + tq.tobytes()
        spd = np.hypot(uu, vv)
        stats[t] = {
            "t_mean_f": round(float(np.nanmean(tt)) * 9 / 5 + 32, 2),
            "spd_mean": round(float(spd.mean()), 4),
            "spd_p99": round(float(np.percentile(spd, 99)), 4),
            "spd_max": round(float(spd.max()), 4),
            "spd_max_cell": int(idx[int(spd.argmax())]),
        }
        fetched += 1
    log.info(f"Fetched and packed {fetched} hours; {len(skipped_zero)} placeholder hours skipped")
    # a placeholder that was stored by an earlier run is dropped too
    for t in [t for t in frames if stats.get(t, {}).get('spd_mean') == 0]:
        frames.pop(t); stats.pop(t, None)
        log.info(f"{t}: removed all-zero frame from the archive")

    # ── 4. rolling window ────────────────────────────────────────────────────
    cutoff = tstr(tparse(new_issued) - timedelta(days=run["rolling_days"]))
    dropped = [t for t in frames if t < cutoff]
    for t in dropped:
        frames.pop(t)
        stats.pop(t, None)
    times = sorted(frames)
    log.info(f"Archive after trim: {len(times)} hours {times[0]} to {times[-1]} (dropped {len(dropped)} before {cutoff})")

    # ── 5. wind ──────────────────────────────────────────────────────────────
    wind_by_time = {t: None for t in times}
    try:
        t0 = tparse(times[0]) - timedelta(days=1)
        t1 = min(tparse(times[-1]), now) + timedelta(days=1)
        q = {
            "station": src["asos_station"], "data": ["sknt", "drct"],
            "year1": t0.year, "month1": t0.month, "day1": t0.day,
            "year2": t1.year, "month2": t1.month, "day2": t1.day,
            "tz": "America/Los_Angeles", "format": "onlycomma", "latlon": "no",
            "missing": "M", "trace": "T", "direct": "no", "report_type": "3",
        }
        csv = fetch(src["asos_url"] + "?" + urllib.parse.urlencode(q, doseq=True), timeout=120).decode("utf-8", "replace")
        obs = []  # (datetime, u_mph, v_mph)
        for line in csv.splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 4:
                continue
            try:
                when = datetime.strptime(parts[1], "%Y-%m-%d %H:%M").replace(tzinfo=PACIFIC)
                kn = float(parts[2]) if parts[2] not in ("M", "") else None
                drct = float(parts[3]) if parts[3] not in ("M", "") else None
            except ValueError:
                continue
            if kn is None:
                continue
            mph = kn * 1.15078
            if mph < 0.5 or drct is None:
                obs.append((when, 0.0, 0.0))
            else:
                toward = math.radians((drct + 180) % 360)
                obs.append((when, mph * math.sin(toward), mph * math.cos(toward)))
        log.info(f"ASOS {src['asos_station']}: {len(obs)} observations {t0.date()} to {t1.date()}")
        window = timedelta(minutes=run.get("wind_window_min", 70))
        for t in times:
            tt = tparse(t)
            near = [(u, v) for when, u, v in obs if abs(when - tt) <= window]
            if near:
                wind_by_time[t] = [round(sum(n[0] for n in near) / len(near), 2), round(sum(n[1] for n in near) / len(near), 2)]
    except Exception as e:
        log.warning(f"Wind fetch failed ({e}); keeping previous wind values where available")
        prev_wind = dict(zip(meta_prev["times"], meta_prev["wind"]))
        wind_by_time = {t: prev_wind.get(t) for t in times}
    missing_wind = sum(1 for t in times if wind_by_time[t] is None and t <= new_issued)
    if missing_wind:
        log.warning(f"{missing_wind} hindcast hours have no airport wind report within the matching window")

    # ── 6. default window: the windiest span in the hindcast ─────────────────
    hind = [t for t in times if t <= new_issued]
    span_h = run.get("event_days", 5) * 24
    best_start, best_score = hind[0], -1.0
    for i, t in enumerate(hind):
        end = tparse(t) + timedelta(hours=span_h)
        block = [wind_by_time[s] for s in hind[i:] if tparse(s) <= end and wind_by_time[s]]
        if tparse(hind[-1]) < end:
            break
        score = sum(math.hypot(*w) for w in block) / max(1, len(block))
        if score > best_score:
            best_score, best_start = score, t
    default_start = best_start
    default_end = min(tstr(tparse(best_start) + timedelta(hours=span_h)), hind[-1])
    log.info(f"Windiest {run.get('event_days', 5)} day span: {default_start} to {default_end} (mean wind {best_score:.1f} mph)")

    # ── 7. QA ────────────────────────────────────────────────────────────────
    step_h = {round((tparse(b) - tparse(a)).total_seconds() / 3600, 1) for a, b in zip(times, times[1:])}
    if step_h - {2.0}:
        log.warning(f"Irregular time steps present: {sorted(step_h)} hours")
    spd_means = [stats[t]["spd_mean"] for t in times]
    if max(spd_means) > 0.6 or min(spd_means) <= 0:
        log.warning(f"Speed means outside the expected range: min {min(spd_means)} max {max(spd_means)} m/s")
    t_means = [stats[t]["t_mean_f"] for t in times]
    if max(t_means) > 80 or min(t_means) < 38:
        log.warning(f"Temperature means outside the expected range: min {min(t_means)} max {max(t_means)} F")
    if len(times) < 24:
        log.warning(f"Archive is short: {len(times)} hours")

    # ── 8. write ─────────────────────────────────────────────────────────────
    meta = dict(meta_prev)
    meta.update({
        "times": times,
        "stats": [stats[t] for t in times],
        "wind": [wind_by_time[t] for t in times],
        "forecast_issued": new_issued,
        "default_start": default_start,
        "default_end": default_end,
        "archive_saved": tstr(now),
        "placeholders": sorted(skipped_zero),
        "rolling_days": run["rolling_days"],
        "data_file": "currents.bin",
        "gzip": True,
        "sources": {
            "model": "UC Davis Tahoe Environmental Research Center (TERC) lake model, Forecasted Conditions",
            "wind": f"Iowa Environmental Mesonet ASOS archive, Lake Tahoe Airport ({src['asos_station']})",
            "shoreline": "UC Davis TERC",
        },
    })
    meta["title"] = meta.get("title", "Lake Tahoe: A Week of Currents")
    body = b"".join(frames[t] for t in times)
    assert len(body) == len(times) * ncell * 3
    (out_dir / "currents.bin").write_bytes(gzip.compress(body, compresslevel=9))
    (out_dir / "meta.json").write_text(json.dumps(meta, separators=(",", ":")), encoding="utf-8")
    log.info(f"Wrote {out_dir / 'meta.json'} and currents.bin ({(out_dir / 'currents.bin').stat().st_size / 1e6:.2f} MB, "
             f"{len(times)} hours, forecast issued {new_issued})")
    log.info("Done")


if __name__ == "__main__":
    main()
