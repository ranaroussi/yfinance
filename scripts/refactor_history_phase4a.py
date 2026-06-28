"""Phase 4a: refactor _reconstruct_intervals_batch only."""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"


def method_range(source: str, name: str) -> tuple[int, int]:
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return fn.lineno, fn.end_lineno


def lines(source: str) -> list[str]:
    return source.splitlines(keepends=True)


def slice_abs(source: str, start: int, end: int) -> str:
    return "".join(lines(source)[start - 1:end])


def replace_method(source: str, name: str, prefix: str, body: str) -> str:
    start, end = method_range(source, name)
    ls = lines(source)
    header = ls[start - 1]
    i = start
    while i < end:
        line = ls[i]
        if line.strip().startswith(('"""', "'''", "@")) or (line.strip() and not line.startswith("        ")):
            header += line
            i += 1
        else:
            break
    return "".join(ls[: start - 1]) + prefix + header + body + "\n" + "".join(ls[end:])


def main():
    source = HISTORY.read_text(encoding="utf-8")
    setup = slice_abs(source, 661, 743)
    groups = slice_abs(source, 745, 788)
    repair = slice_abs(source, 790, 1045)

    helpers = f"""    def _reconstruct_setup(self, df, interval, prepost, tag):
{setup}        return locals()

    def _reconstruct_build_groups(self, loc):
        dts_to_repair = loc['dts_to_repair']
        sub_interval = loc['sub_interval']
        df_good = loc['df_good']
        intraday = loc['intraday']
        min_dt = loc['min_dt']
        logger = loc['logger']
        log_extras = loc['log_extras']
{groups}        return dts_groups

    def _reconstruct_repair_groups(self, loc, dts_groups):
        df = loc['df']
        df_v2 = loc['df_v2']
        interval = loc['interval']
        prepost = loc['prepost']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        intraday = loc['intraday']
        sub_interval = loc['sub_interval']
        td_range = loc['td_range']
        itds = loc['itds']
        min_dt = loc['min_dt']
        price_cols = loc['price_cols']
        data_cols = loc['data_cols']
{repair}

"""

    body = """        loc = self._reconstruct_setup(df, interval, prepost, tag)
        if isinstance(loc, pd.DataFrame):
            return loc
        dts_groups = self._reconstruct_build_groups(loc)
        return self._reconstruct_repair_groups(loc, dts_groups)
"""

    source = replace_method(source, "_reconstruct_intervals_batch", helpers, body)
    HISTORY.write_text(source, encoding="utf-8")
    print("Phase 4a: _reconstruct_intervals_batch refactored")


if __name__ == "__main__":
    main()
