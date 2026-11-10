"""
One-off: sample TRPA's bathymetry onto the lake model grid for the page's depth shading.

Reads the rendered Deep Water Bathymetry (Sonar 2000) and Nearshore Topo Bathy DEM (Green LiDAR 2018)
layers from the Tahoe_Elevation map service, converts their classified colors to a monotonic
shallow-to-deep shade, samples it at every model cell center, smooths it, and writes data/bathy.json.

The service does not accept dynamic raster renderers, so the classified render is the only
pixel access. That gives a stepped shade (six deep classes, five nearshore classes); smoothing
makes it a soft gradient, which is all the page needs for a subtle underlay.

Run: python scripts/build_bathymetry.py
"""
import base64
import io
import json
import math
import sys
from pathlib import Path

import numpy as np
import requests
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.io import get_logger, load_config, project_root  # noqa: E402

# TERC grid georeference (from the Forecasted Conditions page scripts)
CENTER_LAT, CENTER_LON = 39.09363, -120.04734
M_PER_DEG_LAT = 110540.0
M_PER_DEG_LON = 111320.0 * math.cos(math.radians(CENTER_LAT))


def merc(lon, lat):
    r = 6378137.0
    return r * math.radians(lon), r * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


def cell_centers(rows, cols, cell_w, cell_h, rot_deg):
    """lon, lat arrays (rows x cols) for cell centers; row 0 is north."""
    c = np.arange(cols) + 0.5 - cols / 2
    r = rows / 2 - (np.arange(rows) + 0.5)
    xm, ym = np.meshgrid(c * cell_w, r * cell_h)           # east, north meters before rotation
    th = math.radians(rot_deg)
    east = xm * math.cos(th) - ym * math.sin(th)
    north = xm * math.sin(th) + ym * math.cos(th)
    return CENTER_LON + east / M_PER_DEG_LON, CENTER_LAT + north / M_PER_DEG_LAT


def export_layer(base, layer_id, bbox_merc, size):
    params = {
        "bbox": ",".join(f"{v:.3f}" for v in bbox_merc), "bboxSR": 3857, "imageSR": 3857,
        "size": f"{size[0]},{size[1]}", "format": "png32", "transparent": "true",
        "layers": f"show:{layer_id}", "f": "image",
    }
    r = requests.get(f"{base}/export", params=params, timeout=120)
    r.raise_for_status()
    return np.array(Image.open(io.BytesIO(r.content)).convert("RGBA"))


def luminance(rgba):
    return 0.2126 * rgba[..., 0] + 0.7152 * rgba[..., 1] + 0.0722 * rgba[..., 2]


def gaussian_blur(grid, mask, sigma=1.4):
    """Separable Gaussian blur that ignores masked-out cells (normalized convolution)."""
    k = int(3 * sigma)
    x = np.arange(-k, k + 1)
    w = np.exp(-0.5 * (x / sigma) ** 2)
    val = np.where(mask, grid, 0.0)
    wt = mask.astype(float)
    for axis in (0, 1):
        val = np.apply_along_axis(lambda v: np.convolve(v, w, mode="same"), axis, val)
        wt = np.apply_along_axis(lambda v: np.convolve(v, w, mode="same"), axis, wt)
    out = np.where(wt > 0, val / np.maximum(wt, 1e-9), 0.0)
    return np.where(mask, out, 0.0)


def main():
    cfg = load_config()
    log = get_logger("build_bathymetry")
    base = cfg["sources"]["bathymetry_service"]
    data_dir = project_root() / cfg["paths"]["data"]
    meta = json.loads((data_dir / "meta.json").read_text(encoding="utf-8"))
    rows, cols = meta["rows"], meta["cols"]
    idx = []
    r = meta["mask_idx_rle"]
    for i in range(0, len(r), 2):
        idx.extend(range(r[i], r[i] + r[i + 1]))
    idx = np.array(idx)
    mask = np.zeros(rows * cols, bool)
    mask[idx] = True
    mask = mask.reshape(rows, cols)
    log.info(f"Grid {rows}x{cols}, {len(idx)} lake cells; service {base}")

    # Render both layers once over the whole lake at ~25 m pixels (3857)
    x0, y0 = merc(-120.21, 38.90)
    x1, y1 = merc(-119.89, 39.27)
    size = (int((x1 - x0) / 25), int((y1 - y0) / 25))
    deep = export_layer(base, cfg["run"]["bathy_deep_layer"], (x0, y0, x1, y1), size)
    near = export_layer(base, cfg["run"]["bathy_near_layer"], (x0, y0, x1, y1), size)
    log.info(f"Exports {size[0]}x{size[1]} px: deep opaque {(deep[..., 3] > 0).sum():,}, nearshore opaque {(near[..., 3] > 0).sum():,}")

    # Classified colors -> shade. Both renders are blue ramps, lighter = shallower. Rank the distinct colors by
    # luminance so the shade is monotonic in depth class regardless of the exact palette.
    def rank_shade(img):
        opaque = img[..., 3] > 0
        cols_ = np.unique(img[opaque][:, :3], axis=0)
        lum = luminance(cols_.astype(float))
        order = np.argsort(lum)                       # darkest (deepest) first
        key = {tuple(cols_[j]): rank for rank, j in enumerate(order)}
        n = max(1, len(cols_) - 1)
        out = np.full(img.shape[:2], np.nan)
        flat = img[..., :3].reshape(-1, 3)
        ranks = np.array([key.get(tuple(c), -1) for c in flat], float).reshape(img.shape[:2])
        out[opaque] = ranks[opaque] / n
        return out, len(cols_)

    deep_s, n_deep = rank_shade(deep)
    near_s, n_near = rank_shade(near)
    log.info(f"Depth classes: deep {n_deep}, nearshore {n_near}")

    def sample(rot_deg):
        lon, lat = cell_centers(rows, cols, meta["cell_w_m"], meta["cell_h_m"], rot_deg)
        px = (np.vectorize(lambda a, b: merc(a, b)[0])(lon, lat) - x0) / (x1 - x0) * size[0]
        py = (y1 - np.vectorize(lambda a, b: merc(a, b)[1])(lon, lat)) / (y1 - y0) * size[1]
        pi = np.clip(py.astype(int), 0, size[1] - 1)
        pj = np.clip(px.astype(int), 0, size[0] - 1)
        d = deep_s[pi, pj]
        nr = near_s[pi, pj]
        # deep layer covers the basin on a 0..0.78 shade; the nearshore fringe sits above it
        shade = np.where(~np.isnan(d), d * 0.78, np.where(~np.isnan(nr), 0.78 + 0.22 * nr, np.nan))
        return shade

    # The grid is rotated about 1.85 degrees; the sign convention is checked by how many lake cells land on data.
    best = None
    for rot in (meta["rotation_deg"], -meta["rotation_deg"]):
        sh = sample(rot)
        hit = np.isfinite(sh[mask]).mean()
        log.info(f"rotation {rot:+.2f}: {hit:.1%} of lake cells land on bathymetry")
        if best is None or hit > best[1]:
            best = (rot, hit, sh)
    rot, hit, shade = best
    if hit < 0.9:
        log.warning("Fewer than 90 percent of lake cells have bathymetry; check the georeference")

    # Fill the few lake cells without data from their neighbors, then smooth
    filled = np.where(np.isfinite(shade), shade, np.nan)
    missing = mask & ~np.isfinite(filled)
    for _ in range(6):
        if not missing.any():
            break
        pad = np.pad(filled, 1, constant_values=np.nan)
        neigh = np.stack([pad[1 + dr:1 + dr + rows, 1 + dc:1 + dc + cols] for dr in (-1, 0, 1) for dc in (-1, 0, 1)])
        nb = np.nanmean(neigh, axis=0)
        filled = np.where(missing & np.isfinite(nb), nb, filled)
        missing = mask & ~np.isfinite(filled)
    filled = np.where(mask & ~np.isfinite(filled), 1.0, filled)
    smooth = gaussian_blur(np.nan_to_num(filled), mask, sigma=1.4)
    q = np.clip(np.round(smooth * 255), 0, 255).astype(np.uint8)
    cells = q.reshape(-1)[idx]
    log.info(f"Shade range on lake cells {cells.min()} to {cells.max()}, mean {cells.mean():.1f}; rotation used {rot:+.2f}")

    out = {
        "shade_b64": base64.b64encode(cells.tobytes()).decode("ascii"),
        "note": "0 deepest to 255 shallowest, per lake cell in mask order; stepped classes smoothed",
        "source": "TRPA Tahoe_Elevation map service: Deep Water Bathymetry (Sonar 2000) and Nearshore Topo Bathy DEM (Green LiDAR 2018)",
        "built": __import__("datetime").date.today().isoformat(),
    }
    (data_dir / "bathy.json").write_text(json.dumps(out), encoding="utf-8")
    log.info(f"Wrote {data_dir / 'bathy.json'} ({len(cells)} cells)")


if __name__ == "__main__":
    main()
