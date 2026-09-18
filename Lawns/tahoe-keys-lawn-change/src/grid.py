"""
src/grid.py — small raster helpers shared by the notebooks.

Everything in this pipeline lives on one common grid (same CRS, transform,
and shape) so arrays from different years and from the lidar can be compared
pixel for pixel with plain numpy. These helpers define that grid and read or
write rasters on it. Nothing here is clever; it is just the three or four
rasterio calls we would otherwise repeat in every notebook.
"""
import json
import math
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin


def make_grid(bounds, pixel_size, crs_epsg, pad_m=30.0):
    """
    Build the common grid from a bounding box (minx, miny, maxx, maxy) in
    the analysis CRS. The origin is snapped to a multiple of the pixel size
    so re-running always yields the identical grid.
    Returns a dict that can be saved as JSON and reloaded with load_grid().
    """
    minx, miny, maxx, maxy = bounds
    minx = math.floor((minx - pad_m) / pixel_size) * pixel_size
    maxy = math.ceil((maxy + pad_m) / pixel_size) * pixel_size
    maxx = math.ceil((maxx + pad_m) / pixel_size) * pixel_size
    miny = math.floor((miny - pad_m) / pixel_size) * pixel_size
    width = int(round((maxx - minx) / pixel_size))
    height = int(round((maxy - miny) / pixel_size))
    return {
        "crs_epsg": int(crs_epsg),
        "pixel_size": float(pixel_size),
        "origin_x": float(minx),
        "origin_y": float(maxy),
        "width": width,
        "height": height,
    }


def save_grid(grid, path):
    Path(path).write_text(json.dumps(grid, indent=2))


def load_grid(path):
    return json.loads(Path(path).read_text())


def grid_transform(grid):
    return from_origin(grid["origin_x"], grid["origin_y"], grid["pixel_size"], grid["pixel_size"])


def grid_profile(grid, count=1, dtype="uint8", nodata=None):
    """A rasterio profile for writing a raster on the common grid."""
    return {
        "driver": "GTiff",
        "crs": rasterio.crs.CRS.from_epsg(grid["crs_epsg"]),
        "transform": grid_transform(grid),
        "width": grid["width"],
        "height": grid["height"],
        "count": count,
        "dtype": dtype,
        "nodata": nodata,
        "compress": "deflate",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
    }


def write_raster(path, array, grid, nodata=None, tags=None):
    """Write a 2-D or 3-D numpy array to GeoTIFF on the common grid."""
    arr = np.asarray(array)
    if arr.ndim == 2:
        arr = arr[np.newaxis, ...]
    profile = grid_profile(grid, count=arr.shape[0], dtype=str(arr.dtype), nodata=nodata)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr)
        if tags:
            dst.update_tags(**tags)


def read_raster(path, grid=None):
    """Read a raster and (optionally) assert it sits on the common grid."""
    with rasterio.open(path) as src:
        arr = src.read()
        tags = src.tags()
        if grid is not None:
            same = (
                src.width == grid["width"]
                and src.height == grid["height"]
                and abs(src.transform.a - grid["pixel_size"]) < 1e-6
                and abs(src.transform.c - grid["origin_x"]) < 1e-3
                and abs(src.transform.f - grid["origin_y"]) < 1e-3
            )
            if not same:
                raise ValueError(f"{path} is not on the common grid")
    return (arr[0] if arr.shape[0] == 1 else arr), tags


def pixel_area_m2(grid):
    return grid["pixel_size"] ** 2
