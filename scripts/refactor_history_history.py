"""Split history() into nested helpers with nonlocal declarations."""
from __future__ import annotations

import ast
from pathlib import Path

HISTORY = Path(__file__).resolve().parent.parent / "yfinance" / "scrapers" / "history.py"


def method_range(source: str, name: str) -> tuple[int, int, ast.FunctionDef]:
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return fn.lineno, fn.end_lineno, fn


def lines(source: str) -> list[str]:
    return source.splitlines(keepends=True)


def slc(source: str, a: int, b: int) -> str:
    return "".join(lines(source)[a - 1:b])


def find_line(source: str, pattern: str, start: int = 1) -> int:
    for i, line in enumerate(lines(source)[start - 1:], start):
        if pattern in line:
            return i
    raise ValueError(pattern)


def assigned_in_function(fn: ast.FunctionDef) -> list[str]:
    names = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
    return sorted(names)


def main():
    source = HISTORY.read_text(encoding="utf-8")
    a, b, fn = method_range(source, "history")
    params = {arg.arg for arg in fn.args.args}
    nonlocals = ", ".join(n for n in assigned_in_function(fn) if n not in params)
    markers = [
        ("_h_prepare", a + 1, find_line(source, "# Getting data from json", a) - 1),
        ("_h_fetch", find_line(source, "# Getting data from json", a), find_line(source, "fail = False", a) - 1),
        ("_h_validate", find_line(source, "fail = False", a), find_line(source, "# parse quotes", a) - 1),
        ("_h_quotes", find_line(source, "# parse quotes", a), find_line(source, "# actions", a) - 1),
        ("_h_actions", find_line(source, "# actions", a), find_line(source, "# Prepare for combine", a) - 1),
        ("_h_combine", find_line(source, "# Prepare for combine", a), find_line(source, "if repair:", a) - 1),
        ("_h_repair", find_line(source, "if repair:", a), find_line(source, "# Auto/back adjust", a) - 1),
        ("_h_finish", find_line(source, "# Auto/back adjust", a), b - 1),
    ]
    nested = ""
    calls = ""
    for name, sa, sb in markers:
        nested += f"        def {name}():\n            nonlocal {nonlocals}\n{slc(source, sa, sb)}\n"
        calls += f"        {name}()\n"
    header = lines(source)[a - 1]
    i = a
    while i < b:
        line = lines(source)[i]
        if line.strip().startswith(('"""', "'''", "@")) or (line.strip() and not line.startswith("        ")):
            header += line
            i += 1
        else:
            break
    body = f"""        logger = utils.get_yf_logger()
        if raise_errors:
            warnings.warn("'raise_errors' deprecated, do: yf.config.debug.hide_exceptions = False", DeprecationWarning, stacklevel=5)
{nested}{calls}        return df
"""
    ls = lines(source)
    new_source = "".join(ls[: a - 1]) + header + body + "\n" + "".join(ls[b:])
    ast.parse(new_source)
    HISTORY.write_text(new_source, encoding="utf-8")
    print("history nested split ok")


if __name__ == "__main__":
    main()
