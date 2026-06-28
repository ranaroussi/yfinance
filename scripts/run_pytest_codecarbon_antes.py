#!/usr/bin/env python3
"""Executa pytest com CodeCarbon e gera relatório de emissões."""
from codecarbon import EmissionsTracker
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "metrics-before-codecarbon"
OUT_DIR.mkdir(exist_ok=True)

tracker = EmissionsTracker(
    project_name="yfinance_antes",
    output_dir=str(OUT_DIR),
    output_file="emissions_antes",
    log_level="error",
)
tracker.start()
try:
    result = subprocess.run(
        [
            sys.executable, "-m", "pytest", "tests",
            "--cov=yfinance",
            "--cov-report=html:htmlcov_antes",
            "--cov-report=term",
            "--html=metrics-before-pytest/pytest_antes.html",
            "--self-contained-html",
            "-q",
        ],
        cwd=str(ROOT),
    )
finally:
    emissions = tracker.stop()

print(f"Emissions data saved to {OUT_DIR}")
sys.exit(result.returncode)
