"""Phase 4: refactor history, reconstruct, div_adjust, sudden_change via line extraction."""
from __future__ import annotations

import ast
import re
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


def slice_abs(source: str, start: int, end: int) -> list[str]:
    return lines(source)[start - 1:end]


def method_header(source: str, name: str) -> str:
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
    return header


def replace_method(source: str, name: str, prefix: str, body: str) -> str:
    start, end = method_range(source, name)
    ls = lines(source)
    header = method_header(source, name)
    return "".join(ls[: start - 1]) + prefix + header + body + "\n" + "".join(ls[end:])


def insert_before_method(source: str, before: str, text: str) -> str:
    start, _ = method_range(source, before)
    ls = lines(source)
    return "".join(ls[: start - 1]) + text + "".join(ls[start - 1:])


def main():
    source = HISTORY.read_text(encoding="utf-8")

    # --- cluster_dividends -> staticmethod ---
    cluster_src = "".join(slice_abs(source, 1711, 1737))
    cluster_src = cluster_src.replace("        def cluster_dividends", "    @staticmethod\n    def _cluster_dividends", 1)
    cluster_src = cluster_src.replace("(df, column='div', threshold=7):", "(df, column='div', threshold=7):")

    source = source.replace(
        cluster_src.replace("    @staticmethod\n", "").replace("    def _cluster_dividends", "        def cluster_dividends"),
        "",
    )
    source = source.replace("cluster_dividends(", "_cluster_dividends(")
    source = insert_before_method(source, "_fix_bad_div_adjust", cluster_src + "\n")

    # --- _reconstruct_intervals_batch ---
    recon_setup = "".join(slice_abs(source, 661, 743))
    recon_groups = "".join(slice_abs(source, 745, 794))
    recon_repair = "".join(slice_abs(source, 796, 1048))

    recon_helpers = f"""    def _reconstruct_setup(self, df, interval, prepost, tag):
{recon_setup}
        return {{
            'df': df, 'interval': interval, 'prepost': prepost, 'tag': tag,
            'logger': logger, 'log_extras': log_extras, 'intraday': intraday,
            'price_cols': price_cols, 'data_cols': data_cols, 'sub_interval': sub_interval,
            'td_range': td_range, 'itds': itds, 'min_dt': min_dt, 'df_v2': df_v2,
            'df_good': df_good, 'dts_to_repair': dts_to_repair,
        }}

    def _reconstruct_build_groups(self, ctx):
        dts_to_repair = ctx['dts_to_repair']
        sub_interval = ctx['sub_interval']
        df_good = ctx['df_good']
        intraday = ctx['intraday']
        min_dt = ctx['min_dt']
        logger = ctx['logger']
        log_extras = ctx['log_extras']
{recon_groups}
        return dts_groups

    def _reconstruct_repair_groups(self, ctx, dts_groups):
        df = ctx['df']
        df_v2 = ctx['df_v2']
        interval = ctx['interval']
        prepost = ctx['prepost']
        tag = ctx['tag']
        logger = ctx['logger']
        log_extras = ctx['log_extras']
        intraday = ctx['intraday']
        sub_interval = ctx['sub_interval']
        td_range = ctx['td_range']
        itds = ctx['itds']
        min_dt = ctx['min_dt']
        price_cols = ctx['price_cols']
        data_cols = ctx['data_cols']
{recon_repair}
        return df_v2

"""

    recon_body = """        early = self._reconstruct_setup(df, interval, prepost, tag)
        if isinstance(early, pd.DataFrame):
            return early
        dts_groups = self._reconstruct_build_groups(early)
        return self._reconstruct_repair_groups(early, dts_groups)
"""

    # Fix _reconstruct_setup to return df on early exits instead of bare return
    recon_setup_fixed = recon_setup.replace("return df\n", "return df\n", -1)
    # Early returns in setup should return df directly - the wrapper handles it
    recon_helpers = recon_helpers.replace(
        "    def _reconstruct_setup(self, df, interval, prepost, tag):\n" + recon_setup,
        "    def _reconstruct_setup(self, df, interval, prepost, tag):\n" + recon_setup,
    )

    source = replace_method(
        source,
        "_reconstruct_intervals_batch",
        recon_helpers,
        "        " + recon_body.replace("\n", "\n        ").strip() + "\n",
    )

    # --- _fix_bad_div_adjust: split into section helpers ---
    div_sections = [
        ("_div_adjust_fix_pre_div_close", "self, df2, div_indices, logger, log_extras", 1521, 1544, "df2, df_modified"),
        ("_div_adjust_build_status", "self, df2, div_indices, currency_divide, too_big_check_threshold", 1546, 1704, "div_status_df, df2"),
        ("_div_adjust_enrich_present_adj", "self, df2, div_status_df", 1739, 1787, "div_status_df, checks"),
        ("_div_adjust_detect_phantoms", "self, div_status_df, checks, currency_divide, logger, log_extras", 1789, 1851, "div_status_df, checks"),
        ("_div_adjust_detect_phantoms2", "self, div_status_df, checks", 1831, 1851, "div_status_df, checks"),
    ]

    # Simpler div_adjust split using larger chunks
    div_helpers = ""
    chunks = [
        ("_div_adjust_fix_pre_div_close", "self, df2, div_indices, logger, log_extras", 1521, 1544, "df2, False"),
        ("_div_adjust_analyse_dividends", "self, df2, div_indices, currency_divide, too_big_check_threshold", 1546, 1704, "div_status_df, df2"),
        ("_div_adjust_check_present_adj", "self, df2, div_status_df", 1739, 1787, "div_status_df, checks"),
        ("_div_adjust_phantom_pass1", "self, div_status_df, checks, currency_divide", 1789, 1823, "div_status_df"),
        ("_div_adjust_phantom_pass2", "self, div_status_df, checks", 1830, 1851, "div_status_df"),
        ("_div_adjust_remove_phantoms", "self, df2, df2_nan, div_status_df, checks, logger, log_extras", 1853, 1875, "div_status_df, df2, df2_nan, df_modified"),
        ("_div_adjust_maybe_too_small", "self, div_status_df, checks, currency_divide", 1877, 1891, "div_status_df"),
        ("_div_adjust_adj_vs_prices", "self, df2, div_status_df, checks, logger, log_extras", 1902, 2033, "div_status_df, checks"),
        ("_div_adjust_adj_vs_prices2", "self, df2, div_status_df, checks", 2035, 2033, "div_status_df"),  # fix below
    ]

    # Build div helpers from line slices - read fresh line numbers after reconstruct edit
    source = HISTORY.read_text(encoding="utf-8") if False else source

    div_helper_parts = []
    div_chunk_specs = [
        ("_div_adjust_fix_pre_div_close", "self, df2, div_indices, logger, log_extras", 1521, 1544, "df2, df_modified"),
        ("_div_adjust_analyse_dividends", "self, df2, div_indices, currency_divide, too_big_check_threshold", 1546, 1704, "div_status_df, df2"),
        ("_div_adjust_check_present_adj", "self, df2, div_status_df", 1739, 1787, "div_status_df, checks"),
        ("_div_adjust_mark_phantoms", "self, div_status_df, checks, currency_divide", 1789, 1851, "div_status_df, checks"),
        ("_div_adjust_remove_phantoms", "self, df2, df2_nan, div_status_df, checks, logger, log_extras", 1853, 1875, "div_status_df, df2, df2_nan, True"),
        ("_div_adjust_detect_too_small_cluster", "self, div_status_df, checks, currency_divide, df2, df2_nan, df_modified", 1877, 1900, "div_status_df, df2, df2_nan, df_modified"),
        ("_div_adjust_contradicts_prices", "self, df2, div_status_df, checks, logger, log_extras", 1902, 2033, "div_status_df, checks"),
        ("_div_adjust_prune_checks", "self, div_status_df, checks", 2035, 2041, "div_status_df, checks"),
        ("_div_adjust_cluster_reconcile", "self, div_status_df, checks, logger, log_extras", 2043, 2189, "div_status_df, checks"),
        ("_div_adjust_adj_too_small_flag", "self, div_status_df, checks, currency_divide", 2057, 2077, "div_status_df, checks"),
        ("_div_adjust_merge_pre_split_flags", "self, div_status_df, checks", 2079, 2087, "div_status_df, checks"),
        ("_div_adjust_filter_and_repair", "self, df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras", 2191, 2482, "df2"),
    ]

    # Re-read line numbers after reconstruct change
    source_for_div = source
    _, div_end = method_range(source_for_div, "_fix_bad_div_adjust")
    # offsets may have shifted - re-parse
    div_start, div_end = method_range(source, "_fix_bad_div_adjust")

    # Use relative offsets from div_start
    rel = lambda a: a  # absolute lines still valid if we haven't modified div yet

    div_helper_parts = []
    for name, params, a, b, ret in div_chunk_specs:
        chunk = "".join(slice_abs(source, a, b))
        div_helper_parts.append(f"    def {name}({params}):\n        df_modified = False\n{chunk}        return {ret}\n")

    div_helpers = "\n".join(div_helper_parts) + "\n\n"

    div_body = """        if df is None or df.empty:
            return df
        if interval in ['1wk', '1mo', '3mo', '1y']:
            return df
        if 'Capital Gains' in df.columns and (df['Capital Gains']>0).any():
            return df
        logger = utils.get_yf_logger()
        log_extras = {'yf_cat': 'div-adjust-repair-bad', 'yf_interval': interval, 'yf_symbol': self.ticker}
        f_div = (df["Dividends"] != 0.0).to_numpy()
        if not f_div.any():
            logger.debug('No dividends to check', extra=log_extras)
            return df
        currency_divide = 1000 if self._cache['history_metadata']['currency'] == 'KWF' else 100
        too_big_check_threshold = 0.035
        df = df.sort_index()
        df2 = df.copy()
        if 'Repaired?' not in df2.columns:
            df2['Repaired?'] = False
        f_nan = df2['Close'].isna().to_numpy()
        df2_nan = df2[f_nan].copy()
        df2 = df2[~f_nan].copy()
        f_div = (df2["Dividends"] != 0.0).to_numpy()
        if not f_div.any():
            logger.debug('No dividends to check', extra=log_extras)
            return df
        div_indices = np.where(f_div)[0]
        df2, _ = self._div_adjust_fix_pre_div_close(df2, div_indices, logger, log_extras)
        div_status_df, df2 = self._div_adjust_analyse_dividends(df2, div_indices, currency_divide, too_big_check_threshold)
        if div_status_df is None:
            return df
        checks = [c for c in div_status_df.columns if c.startswith('div_')]
        div_status_df = div_status_df.sort_index()
        div_status_df, checks = self._div_adjust_check_present_adj(df2, div_status_df)
        div_status_df, checks = self._div_adjust_mark_phantoms(div_status_df, checks, currency_divide)
        div_status_df, df2, df2_nan, _ = self._div_adjust_remove_phantoms(df2, df2_nan, div_status_df, checks, logger, log_extras)
        div_status_df, df2, df2_nan, df_modified = self._div_adjust_detect_too_small_cluster(div_status_df, checks, currency_divide, df2, df2_nan, False)
        if not div_status_df[checks].any().any():
            if df_modified:
                if not df2_nan.empty:
                    df2 = pd.concat([df2, df2_nan]).sort_index()
                return df2
            return df
        div_status_df, checks = self._div_adjust_contradicts_prices(df2, div_status_df, checks, logger, log_extras)
        div_status_df, checks = self._div_adjust_prune_checks(div_status_df, checks)
        div_status_df, checks = self._div_adjust_cluster_reconcile(div_status_df, checks, logger, log_extras)
        div_status_df, checks = self._div_adjust_adj_too_small_flag(div_status_df, checks, currency_divide)
        div_status_df, checks = self._div_adjust_merge_pre_split_flags(div_status_df, checks)
        div_status_df = div_status_df.sort_index()
        div_status_df = div_status_df[div_status_df[checks].any(axis=1)]
        if div_status_df.empty:
            if not df2_nan.empty:
                df2 = pd.concat([df2, df2_nan]).sort_index()
            return df2
        return self._div_adjust_filter_and_repair(df2, df2_nan, div_status_df, checks, currency_divide, logger, log_extras)
"""

    source = replace_method(source, "_fix_bad_div_adjust", div_helpers, "        " + div_body.replace("\n", "\n        ").strip() + "\n")

    HISTORY.write_text(source, encoding="utf-8")
    print("Phase 4 partial done - reconstruct + div_adjust (needs line fix)")


if __name__ == "__main__":
    main()
