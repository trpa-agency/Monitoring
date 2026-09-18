"""
Tahoe Keys lawn change: run all three notebooks in order.
Run: python scripts/run_pipeline.py
Promote logic from the notebooks into functions here only once it has stabilised.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.io import load_config, get_logger  # noqa: E402


def main():
    cfg = load_config()
    log = get_logger("run_pipeline")
    log.info("=" * 60)
    log.info(f"Starting {cfg['project']['name']}")
    log.info("=" * 60)
    for nb in ["01_extract", "02_classify", "03_change"]:
        path = ROOT / "notebooks" / f"{nb}.ipynb"
        log.info(f"Executing {path.name}")
        try:
            subprocess.run([sys.executable, "-m", "jupyter", "nbconvert", "--to", "notebook", "--execute",
                            str(path), "--output", path.name, "--ExecutePreprocessor.timeout=3600"],
                           check=True, cwd=path.parent)
        except subprocess.CalledProcessError:
            log.exception(f"{nb} failed")
            raise
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
