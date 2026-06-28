"""Final refactor: reconstruct, div_adjust, sudden_change."""
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
    """Lines a..b inclusive (1-indexed)."""
    return "".join(lines(source)[a - 1:b])


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
    out = []
    for line in text.splitlines(keepends=True):
        if line.startswith(prefix):
            out.append(line[spaces:])
        else:
            out.append(line)
    return "".join(out)


def refactor_reconstruct(source: str) -> str:
    grp = slc(source, 756, 767)
    rest = slc(source, 769, 796)
    one = dedent_block(slc(source, 816, 1066).replace("continue", "return df_v2"))
    helpers = f"""    @staticmethod
    def _reconstruct_grp_max_size(sub_interval):
{grp}        return grp_max_size

    def _reconstruct_repair_one_group(self, g, loc):
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
{one}        return df_v2

"""
    groups_body = f"""        dts_to_repair = loc['dts_to_repair']
        sub_interval = loc['sub_interval']
        df_good = loc['df_good']
        intraday = loc['intraday']
        min_dt = loc['min_dt']
        logger = loc['logger']
        log_extras = loc['log_extras']
        dts_groups = [[dts_to_repair[0]]]
        grp_max_size = self._reconstruct_grp_max_size(sub_interval)
        logger.debug(f"grp_max_size = {{grp_max_size}}", extra=log_extras)
{rest}        return dts_groups
"""
    repair_body = """        for g in dts_groups:
            loc['df_v2'] = self._reconstruct_repair_one_group(g, loc)
        return loc['df_v2']
"""
    setup_helpers = f"""    def _reconstruct_init(self, df, interval, prepost, tag):
        logger = utils.get_yf_logger()
        log_extras = {{'yf_cat': 'price-reconstruct', 'yf_interval': interval, 'yf_symbol': self.ticker}}
{slc(source, 663, 687)}        return df, interval, prepost, logger, log_extras, intraday, price_cols, data_cols, itds, nexts, min_lookbacks

    def _reconstruct_pick_subinterval(self, df, interval, nexts, logger, log_extras):
{slc(source, 688, 703)}        return df, sub_interval, td_range

    def _reconstruct_prepare_rows(self, df, data_cols, tag, sub_interval, min_lookbacks, logger, log_extras):
{slc(source, 705, 742)}        return df, dts_to_repair, df_v2, df_good

"""
    setup_body = """        init = self._reconstruct_init(df, interval, prepost, tag)
        if isinstance(init, pd.DataFrame):
            return init
        df, interval, prepost, logger, log_extras, intraday, price_cols, data_cols, itds, nexts, min_lookbacks = init
        picked = self._reconstruct_pick_subinterval(df, interval, nexts, logger, log_extras)
        if isinstance(picked, pd.DataFrame):
            return picked
        df, sub_interval, td_range = picked
        prepared = self._reconstruct_prepare_rows(df, data_cols, tag, sub_interval, min_lookbacks, logger, log_extras)
        if isinstance(prepared, pd.DataFrame):
            return prepared
        df, dts_to_repair, df_v2, df_good = prepared
        return locals()
"""
    source = replace_method(source, "_reconstruct_setup", setup_helpers + helpers, setup_body)
    source = replace_method(source, "_reconstruct_build_groups", "", groups_body)
    source = replace_method(source, "_reconstruct_repair_groups", "", repair_body)
    return source


def find_line(source: str, pattern: str, start: int = 1) -> int:
    for i, line in enumerate(lines(source)[start - 1:], start):
        if pattern in line:
            return i
    raise ValueError(f"pattern not found: {pattern!r}")


def refactor_div_adjust(source: str) -> str:
    da, db = method_range(source, "_fix_bad_div_adjust")
    cluster = slc(source, 1711, 1737)
    if "def cluster_dividends" in cluster:
        static = cluster.replace("        def cluster_dividends", "    @staticmethod\n    def _cluster_dividends", 1)
        source = source.replace(cluster, "")
        source = source.replace("cluster_dividends(", "self._cluster_dividends(")
        da, db = method_range(source, "_fix_bad_div_adjust")
        ls = lines(source)
        source = "".join(ls[: da - 1]) + static + "\n" + "".join(ls[da - 1:])

    da, db = method_range(source, "_fix_bad_div_adjust")
    a_fix = find_line(source, "# Very rarely, the Close", da)
    a_analyse = find_line(source, "# Check dividends if too big", da)
    a_present = find_line(source, "# Check if the present div-adjustment is too big/small, or missing", da)
    a_phantoms = find_line(source, "div_status_df['phantom'] = False", da)
    a_remove = find_line(source, "# Remove phantoms early", da)
    a_after_remove = find_line(source, "        if not div_status_df[checks].any().any():", a_remove)
    a_perfect = find_line(source, "# Perfect", a_after_remove)
    a_contra = find_line(source, "# Check if the present div-adjustment contradicts price action", da)
    a_cluster = find_line(source, "# With small dividends e.g.", da)
    a_apply = find_line(source, "# These arrays track changes for constructing compact log messages", da)

    parts = [
        ("_div_adjust_fix_pre_div_close", "df2, div_indices, logger, log_extras", a_fix, a_analyse - 1, "df2, df_modified"),
        ("_div_adjust_analyse_dividends", "df2, div_indices, currency_divide, too_big_check_threshold", a_analyse, a_present - 1, "div_status_df, df2"),
        ("_div_adjust_check_present_adj", "df2, div_status_df", a_present, a_phantoms - 1, "div_status_df, checks"),
        ("_div_adjust_mark_phantoms", "div_status_df, checks, currency_divide", a_phantoms, a_remove - 1, "div_status_df, checks"),
        ("_div_adjust_remove_phantoms", "df2, df2_nan, div_status_df, checks, logger, log_extras", a_remove, a_after_remove - 1, "div_status_df, df2, df2_nan, df_modified"),
        ("_div_adjust_detect_too_small", "div_status_df, checks, currency_divide", a_after_remove, a_perfect - 2, "div_status_df"),
        ("_div_adjust_contradicts_prices", "df2, div_status_df, checks, logger, log_extras", a_contra, a_cluster - 1, "div_status_df, checks"),
        ("_div_adjust_cluster_reconcile", "div_status_df, checks, logger, log_extras", a_cluster, a_apply - 1, "div_status_df, checks"),
        ("_div_adjust_apply_repairs", "df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras", a_apply, db, "df2"),
    ]
    helpers = ""
    for name, params, a, b, ret in parts:
        body = slc(source, a, b)
        if name == "_div_adjust_fix_pre_div_close":
            body = "        df_modified = False\n        fixed_dates = []\n" + body
        if name == "_div_adjust_analyse_dividends":
            body = "        div_status_df = None\n" + body
        if name == "_div_adjust_check_present_adj":
            body = "        checks = [c for c in div_status_df.columns if c.startswith('div_')]\n        div_status_df = div_status_df.sort_index()\n" + body
        if name == "_div_adjust_remove_phantoms":
            body = "        df_modified = False\n" + body
        helpers += f"    def {name}({params}):\n{body}        return {ret}\n\n"

    head = slc(source, da + 1, a_fix - 1)
    body = head + """
        df2, df_modified = self._div_adjust_fix_pre_div_close(df2, div_indices, logger, log_extras)
        div_status_df, df2 = self._div_adjust_analyse_dividends(df2, div_indices, currency_divide, too_big_check_threshold)
        if div_status_df is None and not df_modified:
            return df
        div_status_df, checks = self._div_adjust_check_present_adj(df2, div_status_df)
        div_status_df, checks = self._div_adjust_mark_phantoms(div_status_df, checks, currency_divide)
        div_status_df, df2, df2_nan, df_modified = self._div_adjust_remove_phantoms(df2, df2_nan, div_status_df, checks, logger, log_extras)
        div_status_df = self._div_adjust_detect_too_small(div_status_df, checks, currency_divide)
        if not div_status_df[checks].any().any():
            if df_modified:
                if not df2_nan.empty:
                    df2 = pd.concat([df2, df2_nan]).sort_index()
                return df2
            return df
        div_status_df, checks = self._div_adjust_contradicts_prices(df2, div_status_df, checks, logger, log_extras)
        div_status_df, checks = self._div_adjust_cluster_reconcile(div_status_df, checks, logger, log_extras)
        div_status_df = div_status_df.sort_index()
        div_status_df = div_status_df[div_status_df[checks].any(axis=1)]
        if div_status_df.empty:
            if not df2_nan.empty:
                df2 = pd.concat([df2, df2_nan]).sort_index()
            return df2
        return self._div_adjust_apply_repairs(df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras)
"""
    return replace_method(source, "_fix_bad_div_adjust", helpers, body)


def refactor_sudden_change(source: str) -> str:
    a, b = method_range(source, "_fix_prices_sudden_change")
    mid = find_line(source, "# Now can detect bad split adjustments", a)
    detect = dedent_block(slc(source, mid, b - 1))
    helpers = f"""    def _sudden_change_prepare(self, df, interval, tz_exchange, change):
{slc(source, a + 1, mid - 1)}        return locals()

    def _sudden_change_detect_and_fix(self, df, loc):
        df2 = loc['df2']
        interval = loc['interval']
        tz_exchange = loc['tz_exchange']
        change = loc['change']
        correct_volume = loc['correct_volume']
        correct_dividend = loc['correct_dividend']
        logger = loc['logger']
        log_extras = loc['log_extras']
{detect}        return df2

"""
    body = """        if df.empty:
            return df
        loc = self._sudden_change_prepare(df, interval, tz_exchange, change)
        if isinstance(loc, pd.DataFrame):
            return loc
        loc['df'] = df
        loc['correct_volume'] = correct_volume
        loc['correct_dividend'] = correct_dividend
        return self._sudden_change_detect_and_fix(df, loc)
"""
    return replace_method(source, "_fix_prices_sudden_change", helpers, body)


def refactor_history(source: str) -> str:
    a, b = method_range(source, "history")
    m_fetch = find_line(source, "# Getting data from json", a)
    m_meta = find_line(source, "# Store the meta data", a)
    m_quotes = find_line(source, "# parse quotes", a)
    m_actions = find_line(source, "# actions", a)
    m_combine = find_line(source, "# Prepare for combine", a)
    m_repair = find_line(source, "if repair:", a)
    m_adjust = find_line(source, "# Auto/back adjust", a)
    m_finish = find_line(source, "# missing rows cleanup", a)

    helpers = f"""    def _history_prepare_request(self, loc, logger, raise_errors):
{slc(source, a + 1, m_fetch - 1)}        return loc

    def _history_fetch(self, loc, logger, raise_errors):
{slc(source, m_fetch, m_meta - 1)}        return loc

    def _history_load_metadata(self, loc, logger, raise_errors):
{slc(source, m_meta, m_quotes - 1)}        return loc

    def _history_process_quotes(self, loc, logger, raise_errors):
{slc(source, m_quotes, m_actions - 1)}        return loc

    def _history_process_actions(self, loc, logger, raise_errors):
{slc(source, m_actions, m_combine - 1)}        return loc

    def _history_merge_and_live(self, loc, logger, raise_errors):
{slc(source, m_combine, m_repair - 1)}        return loc

    def _history_repair_prices(self, loc, logger, raise_errors):
{slc(source, m_repair, m_adjust - 1)}        return loc

    def _history_adjust_finish(self, loc, logger, raise_errors):
{slc(source, m_adjust, m_finish - 1)}        return loc

    def _history_cleanup(self, loc, logger, raise_errors):
{slc(source, m_finish, b - 1)}        return loc

"""

    body = """        logger = utils.get_yf_logger()
        if raise_errors:
            warnings.warn("'raise_errors' deprecated, do: yf.config.debug.hide_exceptions = False", DeprecationWarning, stacklevel=5)
        loc = locals()
        for step in (
            self._history_prepare_request, self._history_fetch, self._history_load_metadata,
            self._history_process_quotes, self._history_process_actions, self._history_merge_and_live,
            self._history_repair_prices, self._history_adjust_finish, self._history_cleanup,
        ):
            loc = step(loc, logger, raise_errors)
            if isinstance(loc, pd.DataFrame):
                return loc
        return loc['df']
"""
    # history slices use bare variable names from history() scope - use exec wrapper instead
    return source


def main():
    source = HISTORY.read_text(encoding="utf-8")
    source = refactor_reconstruct(source)
    ast.parse(source)
    print("reconstruct ok")
    source = refactor_div_adjust(source)
    ast.parse(source)
    print("div_adjust ok")
    source = refactor_sudden_change(source)
    ast.parse(source)
    print("sudden_change ok")
    HISTORY.write_text(source, encoding="utf-8")


if __name__ == "__main__":
    main()
