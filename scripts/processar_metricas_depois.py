#!/usr/bin/env python3
"""Gera métricas Pylint pós-refatoração (R0915, R0902, R0912)."""
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "metrics-after-pylint"
OUT_DIR.mkdir(exist_ok=True)


def main():
    proc = subprocess.run(
        [
            sys.executable, "-m", "pylint", "yfinance",
            "--output-format=json",
            "--reports=n",
            "--score=y",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    text = proc.stdout + proc.stderr
    (OUT_DIR / "pylint_raw_depois.json").write_text(text, encoding="utf-8")

    json_start = text.find("[")
    json_end = text.rfind("]") + 1
    issues = json.loads(text[json_start:json_end]) if json_start >= 0 else []

    score_match = re.search(r"Your code has been rated at ([\d.]+)/10", text)
    score = float(score_match.group(1)) if score_match else None
    (OUT_DIR / "pylint_score_depois.txt").write_text(
        f"Score Pylint: {score}/10\n", encoding="utf-8"
    )

    smell_counter = Counter(i["symbol"] for i in issues)
    refactor_smells = {
        "too-many-statements": {"message-id": "R0915", "count": smell_counter.get("too-many-statements", 0)},
        "too-many-instance-attributes": {"message-id": "R0902", "count": smell_counter.get("too-many-instance-attributes", 0)},
        "too-many-branches": {"message-id": "R0912", "count": smell_counter.get("too-many-branches", 0)},
    }
    (OUT_DIR / "pylint_refactor_depois.json").write_text(
        json.dumps(refactor_smells, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    focused = subprocess.run(
        [
            sys.executable, "-m", "pylint", "yfinance",
            "--disable=all",
            "--enable=R0915,R0902,R0912",
            "--reports=n",
            "--score=y",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    focused_text = focused.stdout + focused.stderr
    (OUT_DIR / "pylint_refactor_focus_depois.txt").write_text(focused_text, encoding="utf-8")

    print("Score:", score)
    print("Refactor smells:", refactor_smells)
    print("Total issues (full pylint):", len(issues))


if __name__ == "__main__":
    main()
