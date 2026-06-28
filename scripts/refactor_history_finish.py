"""Finish pylint 10/10 on history.py - comprehensive splits."""
from __future__ import annotations

import ast
import subprocess
import sys
import textwrap
from pathlib import Path

import refactor_history_phase7 as p7

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"
slc = p7.slc
lines = p7.lines
find_line = p7.find_line
replace_method = p7.replace_method
method_range = p7.method_range
insert_before_method = p7.insert_before_method
dedent_block = p7.dedent_block


def find_exact_line(source: str, text: str, start: int = 1) -> int:
    for i, line in enumerate(lines(source)[start - 1 :], start):
        if line.rstrip("\n") == text.rstrip("\n"):
            return i
    raise ValueError(f"exact line not found: {text!r} from line {start}")


def before_return(source: str, start: int, end: int) -> str:
    ret = end
    while ret >= start:
        if lines(source)[ret - 1].strip().startswith("return"):
            return slc(source, start, ret - 1)
        ret -= 1
    return slc(source, start, end - 1)


def replace_method_range(source: str, name: str, prefix: str, new_method: str) -> str:
    """Replace entire method (signature + body) with prefix helpers + new_method."""
    a, b, _ = method_range(source, name)
    ls = lines(source)
    return "".join(ls[: a - 1]) + prefix + new_method + "\n" + "".join(ls[b:])


def indent_method_body(block: str) -> str:
    block = textwrap.dedent(block)
    return "".join(("        " + line if line.strip() else line) for line in block.splitlines(keepends=True))


def parse_ok(source: str, label: str) -> str:
    ast.parse(source)
    print(f"  ok: {label}")
    return source


def split_analyse_one(source: str) -> str:
    if "_div_adjust_analyse_poss_zero_vol" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_analyse_one")
    m1 = find_line(source, "div_pct = div / df2['Close'].iloc[div_idx-1]", a)
    m2 = find_line(source, "possibilities = []", a)
    m2a = find_line(source, "if (drops==0.0).all()", a)
    m2b = find_line(source, "        else:", m2a)
    m3 = find_line(source, "div_status = {'date': dt", a)
    h = f"""    @staticmethod
    def _div_adjust_analyse_drop_stats(df2, div_idx, div, dt):
{slc(source, m1, m2 - 1)}        return div_pct, drop, drop_2Dmax, typical_volatility, drops, df2

    @staticmethod
    def _div_adjust_analyse_poss_zero_vol(div_pct, drops, df2, div_idx):
        possibilities = []
{dedent_block(slc(source, m2a + 1, m2b - 1), 4)}        return possibilities

    @staticmethod
    def _div_adjust_analyse_poss_data(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold):
        possibilities = []
{dedent_block(slc(source, m2b + 1, m3 - 1), 4)}        return possibilities

    @staticmethod
    def _div_adjust_analyse_possibilities(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold):
        if (drops == 0.0).all() and df2['Volume'].iloc[div_idx] == 0:
            return PriceHistory._div_adjust_analyse_poss_zero_vol(div_pct, drops, df2, div_idx)
        return PriceHistory._div_adjust_analyse_poss_data(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold)

    @staticmethod
    def _div_adjust_analyse_record(div_status_df, div_status):
{before_return(source, find_line(source, "row = pd.DataFrame", a), b)}        return div_status_df

"""
    body = """        div_idx = div_indices[i]
        dt = df2.index[div_idx]
        div = df2['Dividends'].iloc[div_idx]
        if div_idx == 0:
            return div_status_df, df2
        div_pct, drop, drop_2Dmax, typical_volatility, drops, df2 = (
            PriceHistory._div_adjust_analyse_drop_stats(df2, div_idx, div, dt))
        possibilities = PriceHistory._div_adjust_analyse_possibilities(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold)
        div_status = {'date': dt, 'idx': div_idx, 'div': div, '%': div_pct}
        div_status['drop'] = drop
        div_status['drop_2Dmax'] = drop_2Dmax
        div_status['volume'] = df2['Volume'].iloc[div_idx]
        div_status['vol'] = typical_volatility
        div_status['div_too_big'] = False
        div_status['div_too_small'] = False
        div_status['div_pre_split'] = False
        div_status['div_too_big_and_pre_split'] = False
        div_status['div_too_small_and_pre_split'] = False
        if len(possibilities) > 0:
            possibilities = sorted(possibilities, key=lambda k: k['diff'])
            p = possibilities[0]
            div_status[p['state'].replace('-', '_')] = True
        div_status_df = PriceHistory._div_adjust_analyse_record(div_status_df, div_status)
        return div_status_df, df2
"""
    return replace_method(source, "_div_adjust_analyse_one", h, body)


def split_contradicts_scan(source: str) -> str:
    if "_div_adjust_contradicts_scan_core" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_contradicts_scan")
    mid = find_line(source, "if lookahead_idx > lookback_idx:", a)
    h = f"""    @staticmethod
    def _div_adjust_contradicts_scan_core(df2, div_status_df, dt, div, div_idx, div_pct):
{before_return(source, mid, b)}        return div_adj_exceeds_prices, div_date_wrong, div_true_date

"""
    body = f"""{slc(source, a + 1, mid - 1)}        div_adj_exceeds_prices, div_date_wrong, div_true_date = (
            PriceHistory._div_adjust_contradicts_scan_core(
                df2, div_status_df, dt, div, div_idx, div_pct))
        return div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt
"""
    return replace_method(source, "_div_adjust_contradicts_scan", h, body)


def split_contradicts_one(source: str) -> str:
    if "_div_adjust_contradicts_apply_flags" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_contradicts_one")
    mid = find_line(source, "div_adj_is_too_small =", a)
    h = f"""    @staticmethod
    def _div_adjust_contradicts_apply_flags(
            df2, div_status_df, dt, div, div_adj_exceeds_prices, div_date_wrong, div_true_date):
{before_return(source, mid, b)}        return div_status_df

"""
    body = """        div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt = (
            PriceHistory._div_adjust_contradicts_scan(df2, div_status_df, i, checks))
        div = div_status_df['div'].iloc[i]
        return PriceHistory._div_adjust_contradicts_apply_flags(
            df2, div_status_df, dt, div, div_adj_exceeds_prices, div_date_wrong, div_true_date)
"""
    return replace_method(source, "_div_adjust_contradicts_one", h, body)


def split_cluster(source: str) -> str:
    if "_div_adjust_cluster_too_big" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_cluster_inconsistencies")
    loop = find_line(source, "for c in cluster_checks:", a)
    big = find_line(source, "if c == 'div_too_big':", a)
    small = find_line(source, "if c == 'div_too_small':", a)
    adj_m = find_line(source, "if c == 'adj_missing':", a)
    rest = dedent_block(slc(source, adj_m, b - 1), 8)
    h = f"""    @staticmethod
    def _div_adjust_cluster_too_big(div_status_df, cluster, fc, c, n, f_fail, n_fail, pct_fail):
{dedent_block(slc(source, big, small - 1), 8)}        return div_status_df, cluster

    @staticmethod
    def _div_adjust_cluster_too_small(div_status_df, cluster, fc, c, n, f_fail, n_fail, pct_fail):
{dedent_block(slc(source, small, adj_m - 1), 8)}        return div_status_df

    @staticmethod
    def _div_adjust_cluster_one_check(div_status_df, cluster, fc, c, n, checks):
        f_fail = cluster[c].to_numpy()
        n_fail = np.sum(f_fail)
        if n_fail in [0, n]:
            return div_status_df
        pct_fail = n_fail / n
        if c == 'div_too_big':
            div_status_df, cluster = PriceHistory._div_adjust_cluster_too_big(
                div_status_df, cluster, fc, c, n, f_fail, n_fail, pct_fail)
            return div_status_df
        if c == 'div_too_small':
            return PriceHistory._div_adjust_cluster_too_small(
                div_status_df, cluster, fc, c, n, f_fail, n_fail, pct_fail)
{rest}        return div_status_df

"""
    body = f"""{slc(source, a + 1, loop - 1)}        for c in cluster_checks:
            div_status_df = PriceHistory._div_adjust_cluster_one_check(
                div_status_df, cluster, fc, c, n, checks)
        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_cluster_inconsistencies", h, body)


def split_repair_one(source: str) -> str:
    if "_div_adjust_repair_one_adj" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_repair_one")
    body_start = find_line(source, "if div_exceeds_adj or adj_exceeds_div:", a)
    branches = [
        ("elif div_too_small:", "_div_adjust_repair_too_small"),
        ("elif div_too_big:", "_div_adjust_repair_too_big"),
        ("elif adj_missing:", "_div_adjust_repair_adj_missing"),
        ("elif div_date_wrong:", "_div_adjust_repair_date_wrong"),
        ("elif adj_exceeds_prices:", "_div_adjust_repair_adj_prices"),
        ("elif div_pre_split:", "_div_adjust_repair_pre_split"),
    ]
    mid = find_line(source, "elif div_too_small:", body_start)
    helpers = []
    sig = """(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
            div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)"""
    for i, (pat, name) in enumerate(branches):
        start = find_line(source, pat, body_start)
        if i + 1 < len(branches):
            end = find_line(source, branches[i + 1][0], body_start) - 1
        else:
            end = b - 2
        block = slc(source, start, end).replace(pat, pat.replace("elif", "if", 1), 1)
        helpers.append(f"""    @staticmethod
    def {name}{sig}:
{block}        return df2, df2_nan, div_status_df, cluster, div_repairs

""")
    h = f"""    @staticmethod
    def _div_adjust_repair_one_adj(
            df2, df2_nan, cluster, row, dt, enddt, div_repairs, div_exceeds_adj, adj_exceeds_div):
{slc(source, body_start, mid - 1)}        return df2, df2_nan, cluster, div_repairs

{''.join(helpers)}    @staticmethod
    def _div_adjust_repair_one_rest(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
            div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing):
        if div_too_small:
            return PriceHistory._div_adjust_repair_too_small(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        if div_too_big:
            return PriceHistory._div_adjust_repair_too_big(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        if adj_missing:
            return PriceHistory._div_adjust_repair_adj_missing(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        if div_date_wrong:
            return PriceHistory._div_adjust_repair_date_wrong(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        if adj_exceeds_prices:
            return PriceHistory._div_adjust_repair_adj_prices(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        if div_pre_split:
            return PriceHistory._div_adjust_repair_pre_split(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        return df2, df2_nan, div_status_df, cluster, div_repairs

"""
    body = """        if div_exceeds_adj or adj_exceeds_div:
            df2, df2_nan, cluster, div_repairs = PriceHistory._div_adjust_repair_one_adj(
                df2, df2_nan, cluster, row, dt, enddt, div_repairs, div_exceeds_adj, adj_exceeds_div)
        else:
            df2, df2_nan, div_status_df, cluster, div_repairs = PriceHistory._div_adjust_repair_one_rest(
                df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing)
        return df2, df2_nan, div_status_df, cluster, div_repairs
"""
    sig = slc(source, a, body_start - 1)
    return replace_method_range(source, "_div_adjust_repair_one", h, sig + body)


def split_apply_repairs(source: str) -> str:
    if "_div_adjust_apply_row_flags" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_apply_repairs")
    dedup_start = find_line(source, "if div_too_big and adj_exceeds_prices", a)
    loop = find_line(source, "for i in range(len(cluster)-1, -1, -1):", a)
    cluster_empty = find_line(source, "if cluster.empty:", a)
    flags_start = find_line(source, "adj_missing = ", a)
    flags_body = dedent_block(slc(source, flags_start, dedup_start - 1), 8)
    dispatch_body = dedent_block(slc(source, dedup_start, cluster_empty - 1), 8)
    h = f"""    @staticmethod
    def _div_adjust_apply_row_flags(row, checks):
{flags_body}        return (adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small, n_failed_checks)

    @staticmethod
    def _div_adjust_apply_row_dispatch(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, checks, currency_divide,
            div_repairs, flags):
        (adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
         div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small,
         n_failed_checks) = flags
{dispatch_body}        return df2, df2_nan, div_status_df, cluster, div_repairs

"""
    body = f"""{slc(source, a + 1, loop - 1)}            for i in range(len(cluster)-1, -1, -1):
                row = cluster.iloc[i]
                dt = row.name
                enddt = dt - _datetime.timedelta(seconds=1)
                flags = PriceHistory._div_adjust_apply_row_flags(row, checks)
                df2, df2_nan, div_status_df, cluster, div_repairs = (
                    PriceHistory._div_adjust_apply_row_dispatch(
                        df2, df2_nan, div_status_df, cluster, row, dt, enddt, checks,
                        currency_divide, div_repairs, flags))
{slc(source, cluster_empty, b)}"""
    return replace_method(source, "_div_adjust_apply_repairs", h, body)


def split_prepare_setup(source: str) -> str:
    if "_sudden_change_setup_suspend" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_prepare_setup")
    mid = find_line(source, "# If stock is currently suspended", a)
    h = f"""    def _sudden_change_setup_suspend(self, df2, OHLC, logger, log_extras):
{before_return(source, mid, b)}        return df_workings, debug_cols, appears_suspended, idx_latest_active

"""
    body = f"""{slc(source, a + 1, mid - 1)}        df_workings, debug_cols, appears_suspended, idx_latest_active = (
            self._sudden_change_setup_suspend(df2, OHLC, logger, log_extras))
        return locals()
"""
    return replace_method(source, "_sudden_change_prepare_setup", h, body)


def split_prepare_changes(source: str) -> str:
    if "_sudden_change_changes_iqr" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_prepare_changes")
    mid = find_line(source, "# If all 1D changes are closer", a)
    iqr = find_line(source, "# Calculate the true price variance", a)
    h = f"""    def _sudden_change_changes_iqr(self, loc):
        df = loc['df']
        logger = loc['logger']
        log_extras = loc['log_extras']
        fix_type = loc['fix_type']
        interday = loc['interday']
        interval = loc['interval']
        split = loc['split']
        split_rcp = loc['split_rcp']
        _1d_change_denoised = loc['_1d_change_denoised']
{before_return(source, iqr, b)}        return loc

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
{slc(source, a + 1, mid - 1)}        loc.update(locals())
        return self._sudden_change_changes_iqr(loc)
"""
    return replace_method(source, "_sudden_change_prepare_changes", h, body)


def extract_zscore(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_detect_apply_main")
    if "_calc_volume_zscore_weighted" not in slc(source, a, b):
        return source
    vs = find_line(source, "def _calc_volume_zscore_weighted", a)
    ve = find_line(source, "if block_after is not None:", a)
    nested = slc(source, vs, ve - 1)
    inner = dedent_block(nested, 12)
    static = "    @staticmethod\n" + inner.replace(
        "\n    def _calc_volume_zscore(",
        "\n    @staticmethod\n    def _calc_volume_zscore(",
    )
    source = source.replace(nested, "", 1)
    source = source.replace("_calc_volume_zscore_weighted(", "PriceHistory._calc_volume_zscore_weighted(")
    source = source.replace("_calc_volume_zscore(", "PriceHistory._calc_volume_zscore(")
    return insert_before_method(source, "_sudden_change_detect_apply_main", static)


def split_apply_main(source: str) -> str:
    source = extract_zscore(source)
    if "_sudden_change_apply_fp_volume" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_detect_apply_main")
    mid1 = find_line(source, "f = f_down | f_up", a)
    mid2 = find_line(source, "for idx in np.where(f)[0]:", a)
    mid3 = find_line(source, "if not f.any():", a)
    unpack_end = find_line(source, "f_up = _1d_change_x > threshold", a) - 1
    fp_loop = find_line(source, "if f_up_shifts.any():", a)
    h = f"""    def _sudden_change_apply_fp_spikes(self, loc):
        df2 = loc['df2']
        multiday = loc['multiday']
        logger = loc['logger']
        f_up = loc['f_up']
        f_up_ndims = loc['f_up_ndims']
        f_up_shifts = loc['f_up_shifts']
        f_down = loc['f_down']
{before_return(source, fp_loop, mid1)}        loc.update(locals())
        return loc

    def _sudden_change_apply_workings(self, loc):
        df2 = loc['df2']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        price_data_cols = loc.get('price_data_cols', loc['OHLC'])
        r = loc['r']
        f_down = loc['f_down']
        f_up = loc['f_up']
        f = loc['f']
{slc(source, mid1, mid2 - 1)}        loc.update(locals())
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
        f_down = loc['f_down']
        f_up = loc['f_up']
        f = loc['f']
{before_return(source, mid2, mid3)}        loc.update(locals())
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
        f = loc['f']
        split = loc['split']
{before_return(source, mid3, b)}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, unpack_end)}        loc = self._sudden_change_apply_fp_spikes(loc)
        loc = self._sudden_change_apply_workings(loc)
        loc = self._sudden_change_apply_vol_filter(loc)
        return self._sudden_change_apply_abort_check(loc)
"""
    return replace_method(source, "_sudden_change_detect_apply_main", h, body)


def fix_apply_and_split_repair(source: str) -> str:
    if "_sudden_change_detect_apply_repair" in source:
        return split_apply_repair(source)
    a, b, _ = method_range(source, "_sudden_change_detect_apply")
    repair_start = find_line(source, "if idx_latest_active is not None:", a)
    repair_body = before_return(source, repair_start, b)
    h = f"""    def _sudden_change_detect_apply_repair(self, loc):
        df2 = loc['df2']
        df = loc['df']
        interval = loc['interval']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        interday = loc['interday']
        multiday = loc['multiday']
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
        correct_dividend = loc['correct_dividend']
        correct_columns_individually = loc['correct_columns_individually']
        idx_rev_latest_active = loc.get('idx_rev_latest_active')
{repair_body}        loc['df2'] = df2
        return loc

"""
    new_body = """        result = self._sudden_change_detect_apply_main(loc)
        if isinstance(result, pd.DataFrame):
            return result
        loc = result
        loc = self._sudden_change_detect_apply_repair(loc)
        return loc.get('df2', loc['df'])
"""
    source = replace_method(source, "_sudden_change_detect_apply", h, new_body)
    return split_apply_repair(source)


def split_apply_repair(source: str) -> str:
    if "_sudden_change_repair_individual" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_detect_apply_repair")
    ind_start = find_line(source, "if correct_columns_individually:", a)
    else_branch = find_exact_line(source, "        else:", ind_start)
    vol_fix = find_exact_line(source, "        if correct_volume:", else_branch)
    header = slc(source, a + 1, ind_start - 1)
    tail = slc(source, vol_fix, b - 1)
    h = f"""    def _sudden_change_repair_individual(self, loc):
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
        idx_rev_latest_active = loc.get('idx_rev_latest_active')
{indent_method_body(slc(source, ind_start + 1, else_branch - 1))}        loc.update(locals())
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
        idx_rev_latest_active = loc.get('idx_rev_latest_active')
{indent_method_body(slc(source, else_branch + 1, vol_fix - 1))}        loc.update(locals())
        return loc

"""
    body = f"""{header}        if idx_latest_active is not None:
            loc['idx_rev_latest_active'] = df.shape[0] - 1 - idx_latest_active
            logger.debug(
                f'idx_latest_active={{idx_latest_active}}, idx_rev_latest_active={{loc[\"idx_rev_latest_active\"]}}',
                extra=log_extras)
        if correct_columns_individually:
            loc = self._sudden_change_repair_individual(loc)
        else:
            loc = self._sudden_change_repair_combined(loc)
{tail}        loc['df2'] = df2
        return loc
"""
    return replace_method(source, "_sudden_change_detect_apply_repair", h, body)


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
        split_analyse_one,
        split_contradicts_scan,
        split_contradicts_one,
        split_cluster,
        split_repair_one,
        split_apply_repairs,
        split_prepare_setup,
        split_prepare_changes,
        split_apply_main,
        fix_apply_and_split_repair,
    ]
    for step in steps:
        source = step(source)
        source = parse_ok(source, step.__name__)
    HISTORY.write_text(source, encoding="utf-8")
    code, out = run_pylint()
    print(out)
    if code != 0:
        sys.exit(code)
    print("finish ok")


if __name__ == "__main__":
    main()
