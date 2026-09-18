# Environment

This project needs three packages that are not in the stock ArcGIS Pro
environment: `rasterio` (windowed reads of cloud-hosted NAIP, warping onto a
common grid), `pystac-client` and `planetary-computer` (finding and signing
the NAIP scenes). Installing `rasterio` into `arcgispro-py3` directly can
conflict with Esri's bundled GDAL, so this is one of the documented cases
where a one-off clone is the safer choice.

## Setup

Open the ArcGIS Pro Python Command Prompt and run:

```
conda create --name keys-lawn --clone arcgispro-py3
conda activate keys-lawn
conda install -c conda-forge rasterio pystac-client planetary-computer pyyaml python-dotenv
```

Then pick `keys-lawn` as the kernel in Jupyter (or set it as the active
environment in Pro's Package Manager). The first cell of every notebook
prints `sys.executable` so you can confirm.

## What the notebooks use

Already in `arcgispro-py3` and inherited by the clone: `numpy`, `pandas`,
`geopandas`, `shapely`, `scipy`, `scikit-learn`, `scikit-image`, `matplotlib`, `requests`.

Added: `rasterio`, `pystac-client`, `planetary-computer`, `pyyaml`, `python-dotenv`.

`arcpy` is not required by any notebook. The outputs are GeoPackages and
GeoTIFFs that open directly in Pro.

## Network

Notebook 01 reads NAIP straight from Microsoft Planetary Computer over
HTTPS. If the workstation goes through a proxy that blocks
`*.blob.core.windows.net`, download the quarter quads by hand (USDA GeoHub
or EarthExplorer), drop them in `data/raw/naip/local/`, and point
`config.yaml` `sources.naip.local` at them.
