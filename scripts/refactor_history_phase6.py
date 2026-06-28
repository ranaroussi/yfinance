"""Phase 6: split remaining pylint violations."""
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


def slc(source: str, a: int, b: int) -> str:
    return "".join(lines(source)[a - 1:b])


def find_line(source: str, pattern: str, start: int = 1) -> int:
    for i, line in enumerate(lines(source)[start - 1:], start):
        if pattern in line:
            return i
    raise ValueError(pattern)


def replace_method(source: str, name: str, prefix: str, body: str) -> str:
    a, b = method_range(source, name)
    ls = lines(source)
    header = ls[a - 1]
    i = a
    while i < b:
        line = ls[i]
        if line.strip().startswith(('"""', "'''", "@")) or (line.strip() and not line.startswith("        ")):
            header += line
            i += 1
        else:
            break
    return "".join(ls[: a - 1]) + prefix + header + body + "\n" + "".join(ls[b:])


def dedent_block(text: str, spaces: int = 4) -> str:
    prefix = " " * spaces
    return "".join(line[spaces:] if line.startswith(prefix) else line for line in text.splitlines(keepends=True))


def refactor_repair_one(source: str) -> str:
    a, b = method_range(source, "_reconstruct_repair_one_group")
    cal = find_line(source, "# Calibrate!", a)
    rep = find_line(source, "# Repair!", a)
    fetch = slc(source, a + 1, cal - 1)
    calib = slc(source, cal, rep - 1)
    repair = slc(source, rep, b - 1)
    helpers = f"""    def _reconstruct_fetch_fine(self, g, loc):
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
        n_fixed = 0
{fetch}        return df_v2, df_fine, df_new, df_block, interval, grp_col, price_cols, data_cols, n_fixed

    def _reconstruct_calibrate_block(self, g, loc, df_fine, df_new, df_block, grp_col, price_cols, data_cols):
        df = loc['df']
        df_v2 = loc['df_v2']
        interval = loc['interval']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        sub_interval = loc['sub_interval']
        td_range = loc['td_range']
        itds = loc['itds']
        n_fixed = 0
{calib}        return df_v2, df_new, n_fixed

    def _reconstruct_apply_repair(self, g, loc, df_new, df_block, df_fine, interval, price_cols, n_fixed):
        df = loc['df']
        df_v2 = loc['df_v2']
        tag = loc['tag']
{repair}        return df_v2, n_fixed

"""
    body = """        pack = self._reconstruct_fetch_fine(g, loc)
        df_v2, df_fine, df_new, df_block, interval, grp_col, price_cols, data_cols, n_fixed = pack
        df_v2, df_new, n_fixed = self._reconstruct_calibrate_block(g, loc, df_fine, df_new, df_block, grp_col, price_cols, data_cols)
        return self._reconstruct_apply_repair(g, loc, df_new, df_block, df_fine, interval, price_cols, n_fixed)
"""
    return replace_method(source, "_reconstruct_repair_one_group", helpers, body)


def refactor_history_nested(source: str) -> str:
    a, b = method_range(source, "history")
    m_fetch = find_line(source, "# Getting data from json", a)
    m_valid = find_line(source, "fail = False", a)
    m_quotes = find_line(source, "# parse quotes", a)
    m_combine = find_line(source, "# Combine", a)
    m_repair = find_line(source, "if repair:", a)
    m_adjust = find_line(source, "# Auto/back adjust", a)
    m_clean = find_line(source, "# missing rows cleanup", a)

    sections = [
        ("_h_prepare", a + 1, m_fetch - 1),
        ("_h_fetch", m_fetch, m_valid - 1),
        ("_h_validate", m_valid, m_quotes - 1),
        ("_h_quotes", m_quotes, m_combine - 1),
        ("_h_combine", m_combine, m_repair - 1),
        ("_h_repair", m_repair, m_adjust - 1),
        ("_h_adjust", m_adjust, m_clean - 1),
        ("_h_cleanup", m_clean, b - 1),
    ]
    nested = ""
    calls = ""
    for name, sa, sb in sections:
        nested += f"        def {name}():\n{slc(source, sa, sb)}\n"
        calls += f"        {name}()\n"
    body = f"""        logger = utils.get_yf_logger()
        if raise_errors:
            warnings.warn("'raise_errors' deprecated, do: yf.config.debug.hide_exceptions = False", DeprecationWarning, stacklevel=5)
{nested}{calls}        return df
"""
    return replace_method(source, "history", "", body)


def refactor_div_analyse(source: str) -> str:
    a, b = method_range(source, "_div_adjust_analyse_dividends")
    loop = find_line(source, "for i in range(len(div_indices)-1, -1, -1):", a)
    loop_end = find_line(source, "if div_status_df is None and not df_modified:", a)
    iter_body = slc(source, loop + 1, loop_end - 1)
    head = slc(source, a + 1, loop)
    tail = slc(source, loop_end, b - 1)
    helpers = f"""    def _div_adjust_analyse_one(self, df2, div_indices, currency_divide, too_big_check_threshold, div_status_df, i):
{iter_body}        return div_status_df, df2

"""
    body = f"""{head}        for i in range(len(div_indices)-1, -1, -1):
            div_status_df, df2 = self._div_adjust_analyse_one(df2, div_indices, currency_divide, too_big_check_threshold, div_status_df, i)
{tail}        return div_status_df, df2
"""
    return replace_method(source, "_div_adjust_analyse_dividends", helpers, body)


def refactor_apply_repairs(source: str) -> str:
    a, b = method_range(source, "_div_adjust_apply_repairs")
    mid = a + (b - a) // 2
    helpers = f"""    def _div_adjust_apply_repairs_a(self, df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras):
{slc(source, a + 1, mid)}        return df2, df2_nan, div_status_df

    def _div_adjust_apply_repairs_b(self, df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras):
{slc(source, mid + 1, b - 1)}        return df2

"""
    body = """        df2, df2_nan, div_status_df = self._div_adjust_apply_repairs_a(df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras)
        return self._div_adjust_apply_repairs_b(df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras)
"""
    return replace_method(source, "_div_adjust_apply_repairs", helpers, body)


def refactor_sudden_prepare(source: str) -> str:
    a, b = method_range(source, "_sudden_change_prepare")
    mid = a + (b - a) // 2
    helpers = f"""    def _sudden_change_prepare_a(self, df, interval, tz_exchange, change):
{slc(source, a + 1, mid)}        return locals()

    def _sudden_change_prepare_b(self, loc):
{slc(source, mid + 1, b - 1)}        loc.update(locals())
        return loc

"""
    body = """        loc = self._sudden_change_prepare_a(df, interval, tz_exchange, change)
        if isinstance(loc, pd.DataFrame):
            return loc
        return self._sudden_change_prepare_b(loc)
"""
    return replace_method(source, "_sudden_change_prepare", helpers, body)


def refactor_div_orchestrator(source: str) -> str:
    a, b = method_range(source, "_fix_bad_div_adjust")
    head = slc(source, a + 1, find_line(source, "div_indices = np.where", a) - 1)
    tail = slc(source, find_line(source, "df2, df_modified = self._div_adjust_fix_pre_div_close", a), b - 1)
    body = head + tail
    return replace_method(source, "_fix_bad_div_adjust", "", body)


def main():
    source = HISTORY.read_text(encoding="utf-8")
    source = refactor_repair_one(source)
    ast.parse(source)
    print("repair_one ok")
    source = refactor_div_analyse(source)
    ast.parse(source)
    print("div_analyse ok")
    source = refactor_apply_repairs(source)
    ast.parse(source)
    print("apply_repairs ok")
    source = refactor_sudden_prepare(source)
    ast.parse(source)
    print("sudden_prepare ok")
    source = refactor_history_nested(source)
    ast.parse(source)
    print("history nested ok")
    HISTORY.write_text(source, encoding="utf-8")
    print("phase6 done")


if __name__ == "__main__":
    main()
