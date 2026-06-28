"""Phase 11: fix corruption and split remaining pylint smells in history.py."""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import refactor_history_phase7 as p7

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"
slc = p7.slc
find_line = p7.find_line
replace_method = p7.replace_method
method_range = p7.method_range
dedent_block = p7.dedent_block
lines = p7.lines


def remove_method(source: str, name: str) -> str:
    a, b, _ = method_range(source, name)
    return "".join(lines(source)[: a - 1]) + "".join(lines(source)[b:])


def rebuild_contradicts(source: str) -> str:
    """Replace corrupted contradicts block with a working structure."""
    if "_div_adjust_contradicts_prices" not in source:
        return source
    a_prices, b_prices, _ = method_range(source, "_div_adjust_contradicts_prices")
    prices_body = slc(source, a_prices, b_prices)
    if prices_body.count("def _div_adjust_contradicts_prices") > 1:
        # Use the version that finishes with checks cleanup
        pass
    # Find all contradicts_prices methods, keep last one with checks +=
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    good_fn = None
    for fn in cls.body:
        if isinstance(fn, ast.FunctionDef) and fn.name == "_div_adjust_contradicts_prices":
            if "checks +=" in slc(source, fn.lineno, fn.end_lineno):
                good_fn = fn
    if good_fn is None:
        return source

    scan_fn = next(
        (n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_div_adjust_contradicts_scan"),
        None,
    )
    if scan_fn is None:
        return source

    scan_body = slc(source, scan_fn.lineno + 1, scan_fn.end_lineno - 1)
    scan_body = scan_body.replace(
        "        if div_idx == 0:\n            continue\n",
        "        if div_idx == 0:\n            return div_status_df, False, False, pd.NaT, dt\n",
    )
    scan_body = scan_body.replace(
        "        return div_status_df\n",
        "        return div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt\n",
        1,
    )

    prune_start = find_line(source, "# Can prune the space:", good_fn.lineno)
    prune_end = find_line(source, "if 'div_too_big' in div_status_df.columns", good_fn.lineno)
    prune_body = dedent_block(slc(source, prune_start, prune_end + 1), 8)

    tail = slc(source, find_line(source, "checks += ['adj_exceeds_prices'", good_fn.lineno), good_fn.end_lineno - 1)

    block = f"""    @staticmethod
    def _div_adjust_contradicts_scan(df2, div_status_df, i, checks):
{scan_body}
    @staticmethod
    def _div_adjust_contradicts_one(df2, div_status_df, i, checks):
        div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt = (
            PriceHistory._div_adjust_contradicts_scan(df2, div_status_df, i, checks))
        div = div_status_df['div'].iloc[i]
{prune_body}        return div_status_df

    @staticmethod
    def _div_adjust_contradicts_prices(df2, div_status_df, checks, logger, log_extras):
        for i in range(len(div_status_df)):
            div_status_df = PriceHistory._div_adjust_contradicts_one(df2, div_status_df, i, checks)
{tail}        return div_status_df, checks

"""

    # Remove all contradicts_* methods
    for name in ("_div_adjust_contradicts_scan", "_div_adjust_contradicts_one", "_div_adjust_contradicts_prices"):
        while name in source:
            try:
                source = remove_method(source, name)
            except StopIteration:
                break

    ins = method_range(source, "_div_adjust_cluster_setup")[0]
    return "".join(lines(source)[: ins - 1]) + block + "".join(lines(source)[ins - 1 :])


def fix_corruption(source: str) -> str:
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    mark_fns = [
        n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_div_adjust_mark_phantoms"
    ]
    if len(mark_fns) > 1:
        for fn in mark_fns:
            body = slc(source, fn.lineno, fn.end_lineno)
            if "ratio_threshold" not in body:
                a, b = fn.lineno, fn.end_lineno
                source = "".join(lines(source)[: a - 1]) + "".join(lines(source)[b:])
                break

    source = source.replace(
        "        if div_idx == 0:\n            continue\n",
        "        if div_idx == 0:\n            return div_status_df, df2\n",
        1,
    )
    source = source.replace("    @staticmethod\n    @staticmethod\n", "    @staticmethod\n")

    source = rebuild_contradicts(source)

    if "_div_adjust_contradicts_one" not in source:
        source = p7.split_div_contradicts(source)
    if "_div_adjust_contradicts_scan" not in source and "_div_adjust_contradicts_one" in source:
        a, b, _ = method_range(source, "_div_adjust_contradicts_one")
        if "# Can prune the space:" in slc(source, a, b):
            mid = find_line(source, "# Can prune the space:", a)
            h1 = f"""    @staticmethod
    def _div_adjust_contradicts_scan(df2, div_status_df, i, checks):
{slc(source, a + 1, mid - 1)}        return div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt

"""
            body = f"""        div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt = (
            PriceHistory._div_adjust_contradicts_scan(df2, div_status_df, i, checks))
{slc(source, mid, b - 2)}        return div_status_df
"""
            source = replace_method(source, "_div_adjust_contradicts_one", h1, body)

    return source


def fix_reconstruct_apply(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_apply")
    body = slc(source, a + 1, b - 1)
    if "loc['bad_dts']" not in body:
        body = body.replace(
            "bad_dts = df_block.index[(df_block[price_cols + [\"Volume\"]] == tag).to_numpy().any(axis=1)]\n",
            "bad_dts = df_block.index[(df_block[price_cols + [\"Volume\"]] == tag).to_numpy().any(axis=1)]\n"
            "        loc['bad_dts'] = bad_dts\n",
            1,
        )
    body = re.sub(r"\n        no_fine_data_dts = \[\]\n        return self", "\n        return self", body)
    source = replace_method(source, "_reconstruct_repair_apply", "", body)

    a, b, _ = method_range(source, "_reconstruct_repair_apply_rows")
    body = slc(source, a + 1, b - 1)
    if "n_fixed = 0" not in body:
        body = body.replace("bad_dts = loc['bad_dts']\n", "bad_dts = loc['bad_dts']\n        n_fixed = 0\n", 1)
    body = body.replace(
        "                return df_v2\n            df_new_row = df_new.loc[idx]",
        "                continue\n            df_new_row = df_new.loc[idx]",
    )
    if "return df_v2" not in body.split("n_fixed += 1")[-1]:
        body = body.rstrip() + "\n        return df_v2\n"
    source = replace_method(source, "_reconstruct_repair_apply_rows", "", body)
    return source


def split_fetch_request(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_fetch_request")
    if "_reconstruct_fetch_window" in source:
        return source
    mid = find_line(source, "td_1d = _datetime.timedelta(days=1)", a)
    h1 = f"""    def _reconstruct_fetch_window(self, g, loc):
        interval = loc['interval']
        intraday = loc['intraday']
        sub_interval = loc['sub_interval']
        td_range = loc['td_range']
        min_dt = loc['min_dt']
        prepost = loc['prepost']
        logger = loc['logger']
        log_extras = loc['log_extras']
        start_d = loc['start_d']
        start_dt = loc['start_dt']
{slc(source, mid, b - 2)}        return fetch_start, fetch_end, df_fine, log_level, logger

"""
    body = f"""{slc(source, a + 1, mid - 1)}        fetch_start, fetch_end, df_fine, log_level, logger = self._reconstruct_fetch_window(g, loc)
        if df_fine is None or df_fine.empty:
            msg = f"Cannot reconstruct block starting {{start_dt if intraday else start_d}}, too old, Yahoo will reject request for finer-grain data"
            logger.info(msg, extra=log_extras)
            loc['_early'] = True; return loc
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_reconstruct_repair_fetch_request", h1, body)


def split_calibrate_adj(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_calibrate_adj")
    if "_reconstruct_calibrate_adj_close" in source:
        return source
    mid = find_line(source, "if interval == '1d':", a)
    end = find_line(source, "loc.update(locals())", a)
    h1 = f"""    @staticmethod
    def _reconstruct_calibrate_adj_close(df_block, df_new, tag, common_index):
{dedent_block(slc(source, mid + 1, end - 1), 4)}        return df_new

"""
    body = f"""{slc(source, a + 1, mid - 1)}        if interval == '1d':
            df_new = PriceHistory._reconstruct_calibrate_adj_close(
                df_block, df_new, tag, common_index)
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_reconstruct_repair_calibrate_adj", h1, body)


def split_calibrate_ratio(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_calibrate_ratio")
    if "_reconstruct_compute_ratio" in source:
        return source
    mid = find_line(source, "if abs(ratio/0.0001 -1) < 0.01:", a)
    h1 = f"""    @staticmethod
    def _reconstruct_compute_ratio(df_block, df_new, df_fine_grp, common_index, tag, df_v2):
        calib_cols = ['Open', 'Close']
{slc(source, a + 8, mid - 1)}        return ratio, ratio_rcp, df_block, df_new, df_v2

"""
    body = f"""        df = loc['df']
        df_v2 = loc['df_v2']
        df_block = loc['df_block']
        df_new = loc['df_new']
        df_fine_grp = loc['df_fine_grp']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        price_cols = loc['price_cols']
        start_d = loc['start_d']
        common_index = loc['common_index']
        ratio, ratio_rcp, df_block, df_new, df_v2 = PriceHistory._reconstruct_compute_ratio(
            df_block, df_new, df_fine_grp, common_index, tag, df_v2)
{slc(source, mid, b - 2)}        loc.update(locals())
        return loc
"""
    return replace_method(source, "_reconstruct_repair_calibrate_ratio", "", body)


def split_apply_row(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_apply_rows")
    if "_reconstruct_repair_one_row" in source:
        return source
    loop = find_line(source, "for idx in bad_dts:", a)
    inner = find_line(source, "df_bad_row = df.loc[idx]", loop)
    row_body = dedent_block(slc(source, inner, b - 2), 4)
    row_body = row_body.replace("            n_fixed += 1\n", "")
    if "return df_v2, df_fine, 1" not in row_body:
        row_body = row_body.rstrip() + "\n        return df_v2, df_fine, 1\n"
    h1 = f"""    @staticmethod
    def _reconstruct_repair_one_row(df, df_v2, df_new, df_fine, interval, tag, idx):
{row_body}
"""
    body = f"""{slc(source, a + 1, inner - 1)}            df_v2, df_fine, n_inc = PriceHistory._reconstruct_repair_one_row(
                df, df_v2, df_new, df_fine, interval, tag, idx)
            n_fixed += n_inc
        return df_v2
"""
    body = body.replace("            n_fixed += 1\n", "            n_fixed += n_inc\n")
    return replace_method(source, "_reconstruct_repair_apply_rows", h1, body)


def split_analyse_one(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_analyse_one")
    if "_div_adjust_analyse_possibilities" in source:
        return source
    m1 = find_line(source, "div_pct = div / df2['Close'].iloc[div_idx-1]", a)
    m2 = find_line(source, "possibilities = []", a)
    m3 = find_line(source, "div_status = {'date': dt", a)
    h1 = f"""    @staticmethod
    def _div_adjust_analyse_drop_stats(df2, div_idx, div, dt):
{slc(source, m1, m2 - 1)}        return drop, drop_2Dmax, typical_volatility, div_pct, drops, df2

    @staticmethod
    def _div_adjust_analyse_possibilities(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold):
{slc(source, m2, m3 - 1)}        return possibilities

"""
    body = f"""        div_idx = div_indices[i]
        dt = df2.index[div_idx]
        div = df2['Dividends'].iloc[div_idx]
        if div_idx == 0:
            return div_status_df, df2
        drop, drop_2Dmax, typical_volatility, div_pct, drops, df2 = PriceHistory._div_adjust_analyse_drop_stats(
            df2, div_idx, div, dt)
        possibilities = PriceHistory._div_adjust_analyse_possibilities(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold)
{slc(source, m3, b - 2)}        return div_status_df, df2
"""
    return replace_method(source, "_div_adjust_analyse_one", h1, body)


def split_contradicts_scan(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_contradicts_scan")
    if "_div_adjust_contradicts_scan_core" in source:
        return source
    mid = find_line(source, "if lookahead_idx > lookback_idx:", a)
    h1 = f"""    @staticmethod
    def _div_adjust_contradicts_scan_core(df2, div_status_df, i, dt, div, div_idx, div_pct):
{slc(source, mid, b - 2)}        return div_adj_exceeds_prices, div_date_wrong, div_true_date

"""
    body = f"""{slc(source, a + 1, mid - 1)}        div_adj_exceeds_prices, div_date_wrong, div_true_date = (
            PriceHistory._div_adjust_contradicts_scan_core(
                df2, div_status_df, i, dt, div, div_idx, div_pct))
        return div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt
"""
    return replace_method(source, "_div_adjust_contradicts_scan", h1, body)


def split_contradicts_one(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_contradicts_one")
    if "_div_adjust_contradicts_apply_flags" in source:
        return source
    mid = find_line(source, "div_status = {}", a)
    h1 = f"""    @staticmethod
    def _div_adjust_contradicts_apply_flags(
            df2, div_status_df, dt, div, div_adj_exceeds_prices, div_date_wrong, div_true_date):
{slc(source, mid, b - 2)}        return div_status_df

"""
    body = f"""        div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt = (
            PriceHistory._div_adjust_contradicts_scan(df2, div_status_df, i, checks))
        div = div_status_df['div'].iloc[i]
        return PriceHistory._div_adjust_contradicts_apply_flags(
            df2, div_status_df, dt, div, div_adj_exceeds_prices, div_date_wrong, div_true_date)
"""
    return replace_method(source, "_div_adjust_contradicts_one", h1, body)


def split_cluster_inconsistencies(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_cluster_inconsistencies")
    if "_div_adjust_cluster_one_check" in source:
        return source
    loop = find_line(source, "for c in cluster_checks:", a)
    h1 = f"""    @staticmethod
    def _div_adjust_cluster_one_check(div_status_df, cluster, fc, checks, c, n, n_fail, pct_fail):
{dedent_block(slc(source, loop + 1, b - 2), 8)}        return div_status_df

"""
    body = f"""{slc(source, a + 1, loop)}            div_status_df = PriceHistory._div_adjust_cluster_one_check(
                div_status_df, cluster, fc, checks, c, n, n_fail, pct_fail)
        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_cluster_inconsistencies", h1, body)


def split_repair_one(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_repair_one")
    if "_div_adjust_repair_one_adj" in source:
        return source
    mid = find_line(source, "elif div_too_small:", a)
    h1 = f"""    @staticmethod
    def _div_adjust_repair_one_adj(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, div_repairs,
            div_exceeds_adj, adj_exceeds_div):
{slc(source, a + 1, mid - 1)}        return df2, df2_nan, div_status_df, cluster, div_repairs

    @staticmethod
    def _div_adjust_repair_one_div(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, checks, currency_divide, div_repairs,
            div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing):
{slc(source, mid, b - 2)}        return df2, df2_nan, div_status_df, cluster, div_repairs

"""
    body = """        if div_exceeds_adj or adj_exceeds_div:
            return PriceHistory._div_adjust_repair_one_adj(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, div_repairs,
                div_exceeds_adj, adj_exceeds_div)
        return PriceHistory._div_adjust_repair_one_div(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, checks, currency_divide, div_repairs,
            div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
"""
    return replace_method(source, "_div_adjust_repair_one", h1, body)


def split_apply_repairs(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_apply_repairs")
    if "_div_adjust_apply_row_flags" in source:
        return source
    mid = find_line(source, "if n_failed_checks == 1:", a)
    h1 = f"""    @staticmethod
    def _div_adjust_apply_row_flags(row, checks):
{dedent_block(slc(source, find_line(source, "adj_missing = ", a), mid - 1), 8)}
        return (adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small, n_failed_checks)

"""
    loop_start = find_line(source, "for i in range(len(cluster)-1, -1, -1):", a)
    body = f"""{slc(source, a + 1, loop_start + 1)}                row = cluster.iloc[i]
                dt = row.name
                enddt = dt-_datetime.timedelta(seconds=1)
                flags = PriceHistory._div_adjust_apply_row_flags(row, checks)
                (adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
                 div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small,
                 n_failed_checks) = flags
{slc(source, mid, b - 2)}        return df2
"""
    return replace_method(source, "_div_adjust_apply_repairs", h1, body)


def split_prepare_setup(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_prepare_setup")
    if "_sudden_change_setup_workings" in source:
        return source
    mid = find_line(source, "# If stock is currently suspended", a)
    h1 = f"""    def _sudden_change_setup_workings(self, df2, OHLC, logger, log_extras, appears_suspended, idx_latest_active):
{dedent_block(slc(source, mid, b - 2), 8)}        return df_workings, debug_cols

"""
    body = f"""{slc(source, a + 1, mid - 1)}        df_workings, debug_cols = self._sudden_change_setup_workings(
            df2, OHLC, logger, log_extras, appears_suspended, idx_latest_active)
        return locals()
"""
    return replace_method(source, "_sudden_change_prepare_setup", h1, body)


def split_prepare_changes(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_prepare_changes")
    if "_sudden_change_changes_iqr" in source:
        return source
    mid = find_line(source, "# If all 1D changes are closer", a)
    iqr = find_line(source, "# Calculate the true price variance", a)
    h1 = f"""    def _sudden_change_changes_iqr(self, loc):
        df = loc['df']
        logger = loc['logger']
        log_extras = loc['log_extras']
        fix_type = loc['fix_type']
        interday = loc['interday']
        interval = loc['interval']
        split = loc['split']
        split_rcp = loc['split_rcp']
        _1d_change_denoised = loc['_1d_change_denoised']
{dedent_block(slc(source, iqr, b - 2), 8)}        return loc

"""
    body = f"""        df2 = loc['df2']
        interval = loc['interval']
        change = loc['change']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        interday = loc['interday']
        multiday = loc['multiday']
        OHLC = loc['OHLC']
        fix_type = loc['fix_type']
        n = loc['n']
        df = loc['df']
        df_workings = loc['df_workings']
{slc(source, a + 8, mid - 1)}        loc.update(locals())
        return self._sudden_change_changes_iqr(loc)
"""
    return replace_method(source, "_sudden_change_prepare_changes", h1, body)


def extract_volume_zscore(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_detect_apply_main")
    if "_calc_volume_zscore(" not in slc(source, a, b):
        return source
    if "def _calc_volume_zscore_weighted" not in slc(source, a, b):
        return source
    vs = find_line(source, "def _calc_volume_zscore_weighted", a)
    ve = find_line(source, "if block_after is not None:", a)
    nested = slc(source, vs, ve - 1)
    static = nested.replace(
        "                def _calc_volume_zscore_weighted",
        "    @staticmethod\n    def _calc_volume_zscore_weighted",
        1,
    ).replace(
        "                def _calc_volume_zscore",
        "    @staticmethod\n    def _calc_volume_zscore",
        1,
    )
    static = dedent_block(static, 16)
    source = source.replace(nested, "")
    source = source.replace(
        "_calc_volume_zscore_weighted(",
        "PriceHistory._calc_volume_zscore_weighted(",
    ).replace(
        "_calc_volume_zscore(",
        "PriceHistory._calc_volume_zscore(",
    )
    a2, _, _ = method_range(source, "_sudden_change_detect_apply_main")
    return p7.insert_before_method(source, "_sudden_change_detect_apply_main", static)


def split_apply_main(source: str) -> str:
    source = extract_volume_zscore(source)
    a, b, _ = method_range(source, "_sudden_change_detect_apply_main")
    if "_sudden_change_apply_fp_volume" in source:
        return source
    # Remove duplicate loc unpack block
    text = slc(source, a, b)
    if text.count("        df2 = loc['df2']") > 1:
        second = text.find("        df2 = loc['df2']", text.find("        df2 = loc['df2']") + 1)
        third_line = text.find("        f_up = _1d_change_x > threshold", second)
        if third_line > 0:
            trimmed = text[:second] + text[third_line:]
            source = replace_method(source, "_sudden_change_detect_apply_main", "", trimmed.split("\n", 1)[1])
            a, b, _ = method_range(source, "_sudden_change_detect_apply_main")

    mid1 = find_line(source, "f = f_down | f_up", a)
    mid2 = find_line(source, "for idx in np.where(f)[0]:", a)
    mid3 = find_line(source, "if not f.any():", a)
    h1 = f"""    def _sudden_change_apply_fp_volume(self, loc):
        df2 = loc['df2']
        multiday = loc['multiday']
        logger = loc['logger']
        f_up = loc['f_up']
        f_up_ndims = loc['f_up_ndims']
        f_up_shifts = loc['f_up_shifts']
        f_down = loc['f_down']
        _1d_change_x = loc['_1d_change_x']
        threshold = loc['threshold']
{slc(source, find_line(source, "f_up = _1d_change_x > threshold", a), mid1 - 1)}        loc.update(locals())
        return loc

    def _sudden_change_apply_vol_filter(self, loc):
        df2 = loc['df2']
        interval = loc['interval']
        interday = loc['interday']
        logger = loc['logger']
        log_extras = loc['log_extras']
        fix_type = loc['fix_type']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        price_data_cols = loc.get('price_data_cols', loc['OHLC'])
        split_max = loc['split_max']
        debug_cols = loc.get('debug_cols', ['Close'])
{slc(source, mid1, mid3 - 1)}        loc.update(locals())
        return loc

    def _sudden_change_apply_abort_check(self, loc):
        df2 = loc['df2']
        df = loc['df']
        interval = loc['interval']
        change = loc['change']
        logger = loc['logger']
        log_extras = loc['log_extras']
        fix_type = loc['fix_type']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        debug_cols = loc.get('debug_cols', ['Close'])
        start_min = loc.get('start_min')
{slc(source, mid3, b - 2)}        loc.update(locals())
        return loc

"""
    unpack = slc(source, a + 1, find_line(source, "f_up = _1d_change_x > threshold", a) - 1)
    body = f"""{unpack}        loc = self._sudden_change_apply_fp_volume(loc)
        loc = self._sudden_change_apply_vol_filter(loc)
        return self._sudden_change_apply_abort_check(loc)
"""
    return replace_method(source, "_sudden_change_detect_apply_main", h1, body)


def split_apply_repair(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_detect_apply_repair")
    if "_sudden_change_repair_individual" in source:
        return source
    else_branch = find_line(source, "        else:", a)
    h1 = f"""    def _sudden_change_repair_individual(self, loc):
        df2 = loc['df2']
        df = loc['df']
        interval = loc['interval']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        interday = loc['interday']
        OHLC = loc['OHLC']
        n = loc['n']
        fix_type = loc['fix_type']
        f = loc['f']
        f_up = loc['f_up']
        f_down = loc['f_down']
        start_min = loc.get('start_min')
        idx_latest_active = loc.get('idx_latest_active')
        appears_suspended = loc.get('appears_suspended')
        correct_volume = loc['correct_volume']
{dedent_block(slc(source, find_line(source, "if correct_columns_individually:", a) + 1, else_branch - 1), 8)}        loc.update(locals())
        return loc

    def _sudden_change_repair_combined(self, loc):
        df2 = loc['df2']
        df = loc['df']
        interval = loc['interval']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        interday = loc['interday']
        multiday = loc['multiday']
        n = loc['n']
        fix_type = loc['fix_type']
        f = loc['f']
        f_up = loc['f_up']
        f_down = loc['f_down']
        start_min = loc.get('start_min')
        idx_latest_active = loc.get('idx_latest_active')
        appears_suspended = loc.get('appears_suspended')
        correct_volume = loc['correct_volume']
        correct_dividend = loc['correct_dividend']
{dedent_block(slc(source, else_branch + 1, b - 2), 8)}        return loc

"""
    header = slc(source, a + 1, find_line(source, "if idx_latest_active is not None:", a) - 1)
    body = f"""{header}        if correct_columns_individually:
            loc = self._sudden_change_repair_individual(loc)
        else:
            loc = self._sudden_change_repair_combined(loc)
        return loc.get('df2', loc['df2'])
"""
    return replace_method(source, "_sudden_change_detect_apply_repair", h1, body)


def run_pylint() -> tuple[int, str]:
    r = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance/scrapers/history.py",
         "--disable=all", "--enable=R0902,R0915,R0912"],
        cwd=ROOT, capture_output=True, text=True,
    )
    return r.returncode, r.stdout + r.stderr


def main():
    source = HISTORY.read_text(encoding="utf-8")
    steps = [
        fix_reconstruct_apply,
        split_fetch_request,
        split_calibrate_adj,
        split_calibrate_ratio,
        split_apply_row,
    ]
    for step in steps:
        source = step(source)
    ast.parse(source)
    HISTORY.write_text(source, encoding="utf-8")
    code, out = run_pylint()
    print(out)
    if code != 0:
        print(f"pylint exit {code}", file=sys.stderr)
        sys.exit(code)
    print("phase11 ok")


if __name__ == "__main__":
    main()
