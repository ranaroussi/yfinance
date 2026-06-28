#!/usr/bin/env python3
"""Gera gráficos das métricas 'antes da refatoração' para o relatório acadêmico.

Uso:
    python scripts/gerar_graficos_antes.py

Requisitos:
    pip install matplotlib

Saída:
    metrics-before-pylint/grafico_smells_antes.png
    metrics-before-pylint/grafico_categorias_antes.png
    metrics-before-pytest/grafico_testes_antes.png
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
PYLINT_DIR = ROOT / "metrics-before-pylint"
PYTEST_DIR = ROOT / "metrics-before-pytest"


def _bar_chart(labels, values, title, ylabel, out_path, color="#4472C4", top_n=None):
    if top_n:
        pairs = sorted(zip(labels, values), key=lambda x: x[1], reverse=True)[:top_n]
        labels, values = zip(*pairs)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(list(reversed(labels)), list(reversed(values)), color=color)
    ax.set_title(title)
    ax.set_xlabel(ylabel)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Gerado: {out_path}")


def grafico_smells():
    data = json.loads((PYLINT_DIR / "pylint_ranking_smells_antes.json").read_text(encoding="utf-8"))
    labels = list(data.keys())
    values = list(data.values())
    _bar_chart(
        labels, values,
        "Top Code Smells (Pylint) — Antes da Refatoração",
        "Ocorrências",
        PYLINT_DIR / "grafico_smells_antes.png",
        color="#ED7D31",
        top_n=15,
    )


def grafico_categorias():
    data = json.loads((PYLINT_DIR / "pylint_distribuicao_categorias_antes.json").read_text(encoding="utf-8"))
    labels = list(data.keys())
    values = list(data.values())
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
    ax.set_title("Distribuição de Problemas por Categoria — Antes da Refatoração")
    fig.tight_layout()
    out = PYLINT_DIR / "grafico_categorias_antes.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Gerado: {out}")


def grafico_testes():
    passed, failed = 164, 17  # extraído de metrics-before-pytest/pytest_antes.html
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.bar(["Passaram", "Falharam"], [passed, failed], color=["#70AD47", "#C00000"])
    ax.set_title("Resultado dos Testes (Pytest) — Antes da Refatoração")
    ax.set_ylabel("Quantidade")
    for i, v in enumerate([passed, failed]):
        ax.text(i, v + 1, str(v), ha="center")
    fig.tight_layout()
    out = PYTEST_DIR / "grafico_testes_antes.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Gerado: {out}")


if __name__ == "__main__":
    PYLINT_DIR.mkdir(exist_ok=True)
    PYTEST_DIR.mkdir(exist_ok=True)
    grafico_smells()
    grafico_categorias()
    grafico_testes()
