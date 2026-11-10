# Environment

The update script needs only numpy, requests, and pyyaml. No arcpy.

## Local runs

Use the default ArcGIS Pro Python environment, which already has all three. From the repo root in PowerShell (the `&` call operator is required for a quoted path):

    & "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Currents\scripts\update_currents.py

In cmd or Git Bash drop the `&`.

The plain `python` on PATH (3.14) does not have numpy; do not use it for the script.

## CI (GitHub Actions)

`.github/workflows/currents-daily.yml` installs the three packages with pip on Python 3.12 and runs the same script.
