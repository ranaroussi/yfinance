"""Phase 12: finish pylint 10/10 on history.py."""
from __future__ import annotations

import ast
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
insert_before_method = p7.insert_before_method


lines = p7.lines


def before_return(source: str, start: int, end: int, ret_pat: str = "        return ") -> str:
    ret = end
    while ret >= start:
        if lines(source)[ret - 1].strip().startswith("return"):
            return slc(source, start, ret - 1)
        ret -= 1
    return slc(source, start, end - 1)


def parse_ok(source: str, label: str) -> str:
    ast.parse(source)
    print(f"  ok: {label}")
    return source


def split_analyse_one(source: str) -> str:
    if "_div_adjust_analyse_drop_stats" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_analyse_one")
    m1 = find_line(source, "div_pct = div / df2['Close'].iloc[div_idx-1]", a)
    m2 = find_line(source, "possibilities = []", a)
    m3 = find_line(source, "div_status = {'date': dt", a)
    h = f"""    @staticmethod
    def _div_adjust_analyse_drop_stats(df2, div_idx, div, dt):
{slc(source, m1, m2 - 1)}        return div_pct, drop, drop_2Dmax, typical_volatility, drops, df2

    @staticmethod
    def _div_adjust_analyse_possibilities(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold):
{slc(source, m2, m3 - 1)}
        return possibilities

    @staticmethod
    def _div_adjust_analyse_record(div_status_df, div_status):
{before_return(source, find_line(source, "row = pd.DataFrame", a), b)}        return div_status_df

"""
    body = f"""        div_idx = div_indices[i]
        dt = df2.index[div_idx]
        div = df2['Dividends'].iloc[div_idx]
        if div_idx == 0:
            return div_status_df, df2
        div_pct, drop, drop_2Dmax, typical_volatility, drops, df2 = (
            PriceHistory._div_adjust_analyse_drop_stats(df2, div_idx, div, dt))
        possibilities = PriceHistory._div_adjust_analyse_possibilities(
            df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, typical_volatility,
            drops, currency_divide, too_big_check_threshold)
        div_status = {{'date': dt, 'idx':div_idx, 'div': div, '%': div_pct}}
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
    def _div_adjust_contradicts_apply_flags(df2, div_status_df, dt, div, div_adj_exceeds_prices, div_date_wrong, div_true_date):
{before_return(source, mid, b)}        return div_status_df

"""
    body = f"""        div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt = (
            PriceHistory._div_adjust_contradicts_scan(df2, div_status_df, i, checks))
        div = div_status_df['div'].iloc[i]
        return PriceHistory._div_adjust_contradicts_apply_flags(
            df2, div_status_df, dt, div, div_adj_exceeds_prices, div_date_wrong, div_true_date)
"""
    return replace_method(source, "_div_adjust_contradicts_one", h, body)


def split_cluster(source: str) -> str:
    if "_div_adjust_cluster_one_check" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_cluster_inconsistencies")
    loop = find_line(source, "for c in cluster_checks:", a)
    h = f"""    @staticmethod
    def _div_adjust_cluster_one_check(div_status_df, cluster, fc, c, n, checks):
        f_fail = cluster[c].to_numpy()
        n_fail = np.sum(f_fail)
        if n_fail in [0, n]:
            return div_status_df
        pct_fail = n_fail / n
{before_return(source, loop + 1, b)}        return div_status_df

"""
    body = f"""{slc(source, a + 1, loop)}            div_status_df = PriceHistory._div_adjust_cluster_one_check(
                div_status_df, cluster, fc, c, n, checks)
        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_cluster_inconsistencies", h, body)


def split_repair_one(source: str) -> str:
    if "_div_adjust_repair_one_adj" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_repair_one")
    mid = find_line(source, "elif div_too_small:", a)
    h = f"""    @staticmethod
    def _div_adjust_repair_one_adj(
            df2, df2_nan, cluster, row, dt, enddt, div_repairs, div_exceeds_adj, adj_exceeds_div):
{slc(source, a + 1, mid - 1)}        return df2, df2_nan, cluster, div_repairs

    @staticmethod
    def _div_adjust_repair_one_rest(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, currency_divide, div_repairs,
            div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_exceeds_prices, adj_missing):
{before_return(source, mid, b)}        return df2, df2_nan, div_status_df, cluster, div_repairs

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
    return replace_method(source, "_div_adjust_repair_one", h, body)


def split_apply_repairs(source: str) -> str:
    if "_div_adjust_apply_row_flags" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_apply_repairs")
    mid = find_line(source, "if n_failed_checks == 1:", a)
    loop = find_line(source, "for i in range(len(cluster)-1, -1, -1):", a)
    h = f"""    @staticmethod
    def _div_adjust_apply_row_flags(row, checks):
{slc(source, find_line(source, "adj_missing = ", a), mid - 1)}        return (adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
                div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small, n_failed_checks)

"""
    body = f"""{slc(source, a + 1, loop + 1)}                row = cluster.iloc[i]
                dt = row.name
                enddt = dt-_datetime.timedelta(seconds=1)
                flags = PriceHistory._div_adjust_apply_row_flags(row, checks)
                (adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
                 div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small,
                 n_failed_checks) = flags
{before_return(source, mid, b)}        return df2
"""
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
    static = nested.replace(
        "                def _calc_volume_zscore_weighted",
        "    @staticmethod\n    def _calc_volume_zscore_weighted",
        1,
    ).replace(
        "                def _calc_volume_zscore",
        "    @staticmethod\n    def _calc_volume_zscore",
        1,
    )
  # dedent nested defs from 16 to 4 spaces
    out = []
    for line in static.splitlines(keepends=True):
        if line.startswith(" " * 16):
            out.append(line[12:])
        else:
            out.append(line)
    static = "".join(out)
    source = source.replace(nested, "")
    source = source.replace("_calc_volume_zscore_weighted(", "PriceHistory._calc_volume_zscore_weighted(")
    source = source.replace("_calc_volume_zscore(", "PriceHistory._calc_volume_zscore(")
    return insert_before_method(source, "_sudden_change_detect_apply_main", static)


def split_apply_main(source: str) -> str:
    source = extract_zscore(source)
    if "_sudden_change_apply_fp_volume" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_detect_apply_main")
    mid1 = find_line(source, "f = f_down | f_up", a)
    mid2 = find_line(source, "if not f.any():", a)
    h = f"""    def _sudden_change_apply_fp_volume(self, loc):
        df2 = loc['df2']
        multiday = loc['multiday']
        logger = loc['logger']
        f_up = loc.get('f_up')
        f_up_ndims = loc.get('f_up_ndims')
        f_up_shifts = loc.get('f_up_shifts')
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
        r = loc['r']
        f_down = loc['f_down']
        f_up = loc['f_up']
{slc(source, mid1, mid2 - 1)}        loc.update(locals())
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
{before_return(source, mid2, b)}        loc.update(locals())
        return loc

"""
    unpack_end = find_line(source, "f_up = _1d_change_x > threshold", a) - 1
    body = f"""{slc(source, a + 1, unpack_end)}        loc = self._sudden_change_apply_fp_volume(loc)
        loc = self._sudden_change_apply_vol_filter(loc)
        return self._sudden_change_apply_abort_check(loc)
"""
    return replace_method(source, "_sudden_change_detect_apply_main", h, body)


def fix_apply_and_split_repair(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_detect_apply")
    body = slc(source, a + 1, b - 1)
    if "_sudden_change_detect_apply_repair" in source:
        return source
    # body starts with main call then inline repair - extract repair
    main_call = find_line(source, "loc = self._sudden_change_detect_apply_main(loc)", a)
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
    # split repair into individual vs combined
    return split_apply_repair(source)


def split_apply_repair(source: str) -> str:
    if "_sudden_change_repair_individual" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_detect_apply_repair")
    else_branch = find_line(source, "        else:", a)
    ind_start = find_line(source, "if correct_columns_individually:", a)
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
{slc(source, ind_start + 1, else_branch - 1)}        loc.update(locals())
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
{before_return(source, else_branch + 1, b)}        loc.update(locals())
        return loc

"""
    header = slc(source, a + 1, ind_start - 1)
    body = f"""{header}        if idx_latest_active is not None:
            loc['idx_rev_latest_active'] = df.shape[0] - 1 - idx_latest_active
            logger.debug(
                f'idx_latest_active={{idx_latest_active}}, idx_rev_latest_active={{loc[\"idx_rev_latest_active\"]}}',
                extra=log_extras)
        if correct_columns_individually:
            loc = self._sudden_change_repair_individual(loc)
        else:
            loc = self._sudden_change_repair_combined(loc)
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
        try:
            source = step(source)
            source = parse_ok(source, step.__name__)
        except Exception as exc:
            print(f"  FAIL {step.__name__}: {exc}")
            raise
    HISTORY.write_text(source, encoding="utf-8")
    code, out = run_pylint()
    print(out)
    if code != 0:
        sys.exit(code)
    print("phase12 ok")


if __name__ == "__main__":
    main()
