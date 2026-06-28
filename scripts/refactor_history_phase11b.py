"""Phase 11b: remaining pylint splits after phase11a."""
from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import refactor_history_phase11 as p11

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"


def main():
    source = HISTORY.read_text(encoding="utf-8")
    steps = [
        p11.split_apply_main,
        p11.split_apply_repair,
        p11.split_contradicts_scan,
        p11.split_contradicts_one,
        p11.split_cluster_inconsistencies,
        p11.split_repair_one,
        p11.split_apply_repairs,
        p11.split_prepare_setup,
        p11.split_prepare_changes,
        p11.split_analyse_one,
    ]
    for step in steps:
        name = step.__name__
        try:
            source = step(source)
            ast.parse(source)
            print(f"ok: {name}")
        except Exception as exc:
            print(f"skip: {name}: {exc}")
    HISTORY.write_text(source, encoding="utf-8")
    r = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance/scrapers/history.py",
         "--disable=all", "--enable=R0902,R0915,R0912"],
        cwd=ROOT, capture_output=True, text=True,
    )
    print(r.stdout + r.stderr)
    if r.returncode != 0:
        sys.exit(r.returncode)


if __name__ == "__main__":
    main()
