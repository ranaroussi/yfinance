#!/usr/bin/env python3
"""Processa saídas brutas de Radon e Pylint nos formatos do relatório acadêmico."""
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RADON_DIR = ROOT / "metrics-before-radon"
PYLINT_DIR = ROOT / "metrics-before-pylint"


def process_radon():
    cc_data = json.loads((RADON_DIR / "cc_json_antes.json").read_text(encoding="utf-8-sig"))
    mi_data = json.loads((RADON_DIR / "mi_json_antes.json").read_text(encoding="utf-8-sig"))
    raw_text = (RADON_DIR / "raw_antes.txt").read_text(encoding="utf-8-sig")

    # --- CC por arquivo ---
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
    worst5_files = file_rows[:5]
    best5_files = sorted(file_rows, key=lambda r: r["cc_max"])[:5]

    all_funcs.sort(key=lambda f: f["complexity"], reverse=True)
    worst5_funcs = all_funcs[:5]
    best5_funcs = sorted(all_funcs, key=lambda f: f["complexity"])[:5]

    # --- MI por arquivo ---
    mi_rows = []
    for filepath, entry in mi_data.items():
        mi_rows.append({
            "arquivo": filepath.replace("\\", "/"),
            "mi": round(entry["mi"], 2),
            "rank_mi": entry["rank"],
        })
    mi_rows.sort(key=lambda r: r["mi"])
    worst5_mi = mi_rows[:5]
    best5_mi = sorted(mi_rows, key=lambda r: r["mi"], reverse=True)[:5]

    # --- Raw por arquivo + total CSV ---
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

    csv_path = RADON_DIR / "raw_por_arquivo_e_total_antes.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["arquivo", "loc", "lloc", "sloc", "comments", "multi"])
        w.writeheader()
        w.writerows(raw_rows)

    summary = {
        "worst5_files": worst5_files,
        "best5_files": best5_files,
        "worst5_funcs": worst5_funcs,
        "best5_funcs": best5_funcs,
        "worst5_mi": worst5_mi,
        "best5_mi": best5_mi,
        "total_raw": raw_rows[-1] if raw_rows else {},
    }
    (RADON_DIR / "resumo_antes.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def process_pylint():
    import subprocess
    import sys

    raw_path = PYLINT_DIR / "pylint_raw_antes.json"
    text = raw_path.read_text(encoding="utf-8-sig")
    # pylint may append score line after JSON on stderr capture
    json_start = text.find("[")
    json_end = text.rfind("]") + 1
    issues = json.loads(text[json_start:json_end])

    score_proc = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance", "--reports=n", "--score=y"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
     )
    score_text = score_proc.stdout + score_proc.stderr
    score_match = re.search(r"Your code has been rated at ([\d.]+)/10", score_text)
    score = float(score_match.group(1)) if score_match else None
    (PYLINT_DIR / "pylint_score_antes.txt").write_text(
        f"Score Pylint: {score}/10\n", encoding="utf-8"
    )

    smell_counter = Counter(i["symbol"] for i in issues)
    (PYLINT_DIR / "pylint_ranking_smells_antes.json").write_text(
        json.dumps(dict(smell_counter.most_common()), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    cat_counter = Counter(i["type"] for i in issues)
    (PYLINT_DIR / "pylint_distribuicao_categorias_antes.json").write_text(
        json.dumps(dict(cat_counter), indent=2, ensure_ascii=False), encoding="utf-8"
    )

    file_counter = Counter(i["path"].replace("\\", "/") for i in issues)
    top10_files = [
        {"arquivo": f, "ocorrencias": c}
        for f, c in file_counter.most_common(10)
    ]
    (PYLINT_DIR / "pylint_arquivos_criticos_antes.json").write_text(
        json.dumps(top10_files, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    refactor_smells = {
        "too-many-statements": {"message-id": "R0915", "count": smell_counter.get("too-many-statements", 0)},
        "too-many-instance-attributes": {"message-id": "R0902", "count": smell_counter.get("too-many-instance-attributes", 0)},
        "too-many-branches": {"message-id": "R0912", "count": smell_counter.get("too-many-branches", 0)},
    }
    (PYLINT_DIR / "pylint_refactor_antes.json").write_text(
        json.dumps(refactor_smells, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    return {
        "score": score,
        "smells": dict(smell_counter.most_common(15)),
        "categories": dict(cat_counter),
        "top10_files": top10_files,
        "refactor": refactor_smells,
        "total_issues": len(issues),
    }


if __name__ == "__main__":
    radon = process_radon()
    pylint = process_pylint()
    print("Radon total:", radon["total_raw"])
    print("Pylint score:", pylint["score"])
    print("Refactor smells:", pylint["refactor"])
