#!/usr/bin/env python3
"""Coleta e processa métricas pós-refatoração (espelho da etapa antes)."""
import csv
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RADON_DIR = ROOT / "metrics-after-radon"
PYLINT_DIR = ROOT / "metrics-after-pylint"
PYTEST_DIR = ROOT / "metrics-after-pytest"
CODECARBON_DIR = ROOT / "metrics-after-codecarbon"

for d in (RADON_DIR, PYLINT_DIR, PYTEST_DIR, CODECARBON_DIR):
    d.mkdir(exist_ok=True)


def run_radon_collect():
    cmds = [
        ([sys.executable, "-m", "radon", "cc", "yfinance", "-a", "-s", "-j"],
         RADON_DIR / "cc_json_depois.json"),
        ([sys.executable, "-m", "radon", "mi", "yfinance", "-s", "-j"],
         RADON_DIR / "mi_json_depois.json"),
        ([sys.executable, "-m", "radon", "raw", "yfinance", "-s"],
         RADON_DIR / "raw_depois.txt"),
    ]
    for cmd, out in cmds:
        proc = subprocess.run(
            cmd, cwd=str(ROOT), capture_output=True, text=True,
            encoding="utf-8", errors="replace",
        )
        text = proc.stdout + proc.stderr
        out.write_text(text, encoding="utf-8")
        if proc.returncode not in (0,):
            print(f"radon warning ({out.name}): exit {proc.returncode}")


def process_radon():
    cc_data = json.loads((RADON_DIR / "cc_json_depois.json").read_text(encoding="utf-8-sig"))
    mi_data = json.loads((RADON_DIR / "mi_json_depois.json").read_text(encoding="utf-8-sig"))
    raw_text = (RADON_DIR / "raw_depois.txt").read_text(encoding="utf-8-sig")

    file_rows = []
    all_funcs = []
    for filepath, blocks in cc_data.items():
        funcs = [b for b in blocks if b.get("type") in ("function", "method")]
        if not funcs:
            continue
        complexities = [b["complexity"] for b in funcs]
        worst = max(funcs, key=lambda b: b["complexity"])
        file_rows.append({
            "arquivo": filepath.replace("\\", "/"),
            "funcoes": len(funcs),
            "cc_media": round(sum(complexities) / len(complexities), 2),
            "cc_max": max(complexities),
            "cc_soma": sum(complexities),
            "pior_rank": worst["rank"],
            "pior_funcao": worst["name"],
        })
        for b in funcs:
            all_funcs.append({
                "arquivo": filepath.replace("\\", "/"),
                "tipo": b["type"],
                "nome": b["name"],
                "rank_cc": b["rank"],
                "complexity": b["complexity"],
            })

    file_rows.sort(key=lambda r: r["cc_max"], reverse=True)
    mi_rows = [
        {
            "arquivo": filepath.replace("\\", "/"),
            "mi": round(entry["mi"], 2),
            "rank_mi": entry["rank"],
        }
        for filepath, entry in mi_data.items()
    ]
    mi_rows.sort(key=lambda r: r["mi"])

    raw_rows = []
    current = None
    for line in raw_text.splitlines():
        line = line.strip()
        if line.startswith("yfinance"):
            current = {"arquivo": line.replace("\\", "/")}
        elif current and line.startswith("LOC:"):
            current["loc"] = int(line.split(":")[1].strip())
        elif current and line.startswith("LLOC:"):
            current["lloc"] = int(line.split(":")[1].strip())
        elif current and line.startswith("SLOC:"):
            current["sloc"] = int(line.split(":")[1].strip())
        elif current and line.startswith("Comments:"):
            current["comments"] = int(line.split(":")[1].strip())
        elif current and line.startswith("Multi:"):
            current["multi"] = int(line.split(":")[1].strip())
            raw_rows.append(current)
            current = None
        elif line == "** Total **":
            current = {"arquivo": "TOTAL"}
        elif current and current.get("arquivo") == "TOTAL":
            if line.startswith("LOC:"):
                current["loc"] = int(line.split(":")[1].strip())
            elif line.startswith("LLOC:"):
                current["lloc"] = int(line.split(":")[1].strip())
            elif line.startswith("SLOC:"):
                current["sloc"] = int(line.split(":")[1].strip())
            elif line.startswith("Comments:"):
                current["comments"] = int(line.split(":")[1].strip())
            elif line.startswith("Multi:"):
                current["multi"] = int(line.split(":")[1].strip())
                raw_rows.append(current)

    csv_path = RADON_DIR / "raw_por_arquivo_e_total_depois.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["arquivo", "loc", "lloc", "sloc", "comments", "multi"])
        w.writeheader()
        w.writerows(raw_rows)

    all_funcs.sort(key=lambda f: f["complexity"], reverse=True)
    summary = {
        "worst5_files": file_rows[:5],
        "best5_files": sorted(file_rows, key=lambda r: r["cc_max"])[:5],
        "worst5_funcs": all_funcs[:5],
        "best5_funcs": sorted(all_funcs, key=lambda f: f["complexity"])[:5],
        "worst5_mi": mi_rows[:5],
        "best5_mi": sorted(mi_rows, key=lambda r: r["mi"], reverse=True)[:5],
        "total_raw": raw_rows[-1] if raw_rows else {},
    }
    (RADON_DIR / "resumo_depois.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def run_pylint_collect():
    proc = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance",
         "--output-format=json", "--reports=n", "--score=y"],
        cwd=str(ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    text = proc.stdout + proc.stderr
    (PYLINT_DIR / "pylint_raw_depois.json").write_text(text, encoding="utf-8")
    return text


def process_pylint(raw_text=None):
    if raw_text is None:
        raw_text = (PYLINT_DIR / "pylint_raw_depois.json").read_text(encoding="utf-8-sig")
    json_start = raw_text.find("[")
    json_end = raw_text.rfind("]") + 1
    issues = json.loads(raw_text[json_start:json_end]) if json_start >= 0 else []

    score_proc = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance", "--reports=n", "--score=y"],
        cwd=str(ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    score_text = score_proc.stdout + score_proc.stderr
    score_match = re.search(r"Your code has been rated at ([\d.]+)/10", score_text)
    score = float(score_match.group(1)) if score_match else None
    (PYLINT_DIR / "pylint_score_depois.txt").write_text(
        f"Score Pylint: {score}/10\n", encoding="utf-8"
    )

    smell_counter = Counter(i["symbol"] for i in issues)
    (PYLINT_DIR / "pylint_ranking_smells_depois.json").write_text(
        json.dumps(dict(smell_counter.most_common()), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    cat_counter = Counter(i["type"] for i in issues)
    (PYLINT_DIR / "pylint_distribuicao_categorias_depois.json").write_text(
        json.dumps(dict(cat_counter), indent=2, ensure_ascii=False), encoding="utf-8",
    )
    top10_files = [
        {"arquivo": f, "ocorrencias": c}
        for f, c in Counter(i["path"].replace("\\", "/") for i in issues).most_common(10)
    ]
    (PYLINT_DIR / "pylint_arquivos_criticos_depois.json").write_text(
        json.dumps(top10_files, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    refactor_smells = {
        "too-many-statements": {"message-id": "R0915", "count": smell_counter.get("too-many-statements", 0)},
        "too-many-instance-attributes": {"message-id": "R0902", "count": smell_counter.get("too-many-instance-attributes", 0)},
        "too-many-branches": {"message-id": "R0912", "count": smell_counter.get("too-many-branches", 0)},
    }
    (PYLINT_DIR / "pylint_refactor_depois.json").write_text(
        json.dumps(refactor_smells, indent=2, ensure_ascii=False), encoding="utf-8",
    )

    focused = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance",
         "--disable=all", "--enable=R0915,R0902,R0912", "--reports=n", "--score=y"],
        cwd=str(ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    (PYLINT_DIR / "pylint_refactor_focus_depois.txt").write_text(
        focused.stdout + focused.stderr, encoding="utf-8",
    )
    return {"score": score, "refactor": refactor_smells, "total_issues": len(issues)}


def run_pytest():
    cmd = [
        sys.executable, "-m", "pytest", "tests",
        "--cov=yfinance",
        "--cov-report=html:htmlcov_depois",
        "--cov-report=term",
        "-q",
    ]
    html_report = PYTEST_DIR / "pytest_depois.html"
    try:
        import pytest_html  # noqa: F401
        cmd.extend([
            f"--html={html_report.relative_to(ROOT).as_posix()}",
            "--self-contained-html",
        ])
    except ImportError:
        print("pytest-html não instalado; relatório HTML de testes omitido")
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return proc.returncode


def run_codecarbon():
    from codecarbon import EmissionsTracker
    tracker = EmissionsTracker(
        project_name="yfinance_depois",
        output_dir=str(CODECARBON_DIR),
        output_file="emissions_depois",
        log_level="error",
    )
    tracker.start()
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests", "-q"],
            cwd=str(ROOT),
        )
    finally:
        tracker.stop()
    return result.returncode


def main():
    steps = sys.argv[1:] or ["radon", "pylint", "pytest", "codecarbon"]
    results = {}

    if "radon" in steps:
        print("=== Radon ===")
        run_radon_collect()
        results["radon"] = process_radon()
        print("Radon total:", results["radon"]["total_raw"])

    if "pylint" in steps:
        print("=== Pylint ===")
        run_pylint_collect()
        results["pylint"] = process_pylint()
        print("Pylint score:", results["pylint"]["score"])
        print("Refactor smells:", results["pylint"]["refactor"])

    if "pytest" in steps:
        print("=== Pytest ===")
        rc = run_pytest()
        results["pytest_rc"] = rc
        print("Pytest exit code:", rc)

    if "codecarbon" in steps:
        print("=== CodeCarbon ===")
        rc = run_codecarbon()
        results["codecarbon_rc"] = rc
        print("CodeCarbon/pytest exit code:", rc)
        print(f"Emissions saved to {CODECARBON_DIR}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
