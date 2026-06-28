"""Phase 14: final pylint splits on history.py."""
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
dedent_block = p7.dedent_block


def before_return(source: str, start: int, end: int) -> str:
    ret = end
    while ret >= start:
        if lines(source)[ret - 1].strip().startswith("return"):
            return slc(source, start, ret - 1)
        ret -= 1
    return slc(source, start, end - 1)


def find_exact_line(source: str, text: str, start: int = 1) -> int:
    for i, line in enumerate(lines(source)[start - 1 :], start):
        if line.rstrip("\n") == text.rstrip("\n"):
            return i
    raise ValueError(f"exact line not found: {text!r} from line {start}")


def replace_method_range(source: str, name: str, prefix: str, new_method: str) -> str:
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


def fix_contradicts_apply(source: str) -> str:
    while source.count("def _div_adjust_contradicts_prices") > 1:
        a, b, _ = method_range(source, "_div_adjust_contradicts_prices")
        if "return df2, div_status_df, i, checks" in slc(source, a, b):
            ls = lines(source)
            source = "".join(ls[: a - 1]) + "".join(ls[b:])
        else:
            break
    a, b, _ = method_range(source, "_div_adjust_contradicts_prices")
    if "            # Can prune the space:" not in slc(source, a, b):
        return source
    dup = find_line(source, "            # Can prune the space:", a)
    checks_add = find_line(source, "        checks +=", a)
    block = indent_method_body(slc(source, dup, checks_add - 1))
    aa, ab, _ = method_range(source, "_div_adjust_contradicts_apply_flags")
    sig = slc(source, aa, find_line(source, "return div_status_df", aa) - 1)
    new_apply = sig + block + "        return div_status_df\n"
    source = replace_method_range(source, "_div_adjust_contradicts_apply_flags", "", new_apply)
    tail = slc(source, checks_add, b - 1)
    body = f"""        for i in range(len(div_status_df)):
            div_status_df = PriceHistory._div_adjust_contradicts_one(df2, div_status_df, i, checks)
{tail}        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_contradicts_prices", "", body)


def split_poss_data(source: str) -> str:
    if "_div_adjust_analyse_poss_too_big" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_analyse_poss_data")
    m1 = find_line(source, "# Check for div-too-big", a)
    m2 = find_line(source, "# Check for div-too-small", a)
    body_start = find_line(source, "possibilities = []", a)
    sig = slc(source, a, body_start - 1)
    sig_args = (
        "(possibilities, df2, div_idx, div, dt, div_pct, drop, drop_2Dmax, "
        "typical_volatility, currency_divide, too_big_check_threshold, div_postSplit)"
    )
    h = f"""    @staticmethod
    def _div_adjust_analyse_poss_presplit(possibilities, df2, div, dt, drop, drop_2Dmax, typical_volatility):
        div_too_big_improvement_threshold = 2
        split = df2['Stock Splits'].loc[dt]
        if split == 0.0:
            return possibilities, None
        div_postSplit = div / split
        if div_postSplit > div:
            _drop = drop - typical_volatility
        else:
            _drop = drop_2Dmax
        if _drop > 0:
            diff = abs(div - _drop)
            diff_postSplit = abs(div_postSplit - _drop)
            if (diff_postSplit * div_too_big_improvement_threshold) <= diff:
                possibilities.append({{'state': 'div-pre-split', 'diff': diff_postSplit}})
        return possibilities, div_postSplit

    @staticmethod
    def _div_adjust_analyse_poss_too_big{sig_args}:
        div_too_big_improvement_threshold = 2
{slc(source, m1, m2 - 1)}        return possibilities

    @staticmethod
    def _div_adjust_analyse_poss_too_small{sig_args}:
        div_too_small_improvement_threshold = 1
        div_too_big_improvement_threshold = 2
{before_return(source, m2, b)}        return possibilities

"""
    body = """        possibilities = []
        possibilities, div_postSplit = PriceHistory._div_adjust_analyse_poss_presplit(
            possibilities, df2, div, dt, drop, drop_2Dmax, typical_volatility)
        possibilities = PriceHistory._div_adjust_analyse_poss_too_big(
            possibilities, df2, div_idx, div, dt, div_pct, drop, drop_2Dmax,
            typical_volatility, currency_divide, too_big_check_threshold, div_postSplit)
        return PriceHistory._div_adjust_analyse_poss_too_small(
            possibilities, df2, div_idx, div, dt, div_pct, drop, drop_2Dmax,
            typical_volatility, currency_divide, too_big_check_threshold, div_postSplit)
"""
    return replace_method_range(source, "_div_adjust_analyse_poss_data", h, sig + body)


def split_scan_core(source: str) -> str:
    if "_div_adjust_contradicts_scan_loop" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_contradicts_scan_core")
    loop = find_line(source, "for idx in indices:", a)
    ret = find_exact_line(source, "        return div_adj_exceeds_prices, div_date_wrong, div_true_date", a)
    body_start = find_line(source, "if lookahead_idx > lookback_idx:", a)
    sig = slc(source, a, body_start - 1)
    h = f"""    @staticmethod
    def _div_adjust_contradicts_scan_loop(
            df2, div_status_df, dt, div, deltas, adjDiv,
            div_adj_exceeds_prices, div_date_wrong, div_true_date):
{indent_method_body(slc(source, loop + 1, ret - 1))}        return div_adj_exceeds_prices, div_date_wrong, div_true_date

"""
    body = f"""{slc(source, body_start, loop - 1)}                    for idx in indices:
                        div_adj_exceeds_prices, div_date_wrong, div_true_date = (
                            PriceHistory._div_adjust_contradicts_scan_loop(
                                df2, div_status_df, dt, div, deltas, adjDiv,
                                div_adj_exceeds_prices, div_date_wrong, div_true_date))
{slc(source, ret, b)}"""
    return replace_method_range(source, "_div_adjust_contradicts_scan_core", h, sig + body)


def split_apply_flags(source: str) -> str:
    if "_div_adjust_contradicts_merge_flags" in source:
        return source
    a, b, _ = method_range(source, "_div_adjust_contradicts_apply_flags")
    mid = find_line(source, "for k,v in div_status.items():", a)
    body_start = find_line(source, "div_adj_is_too_small =", a)
    sig = slc(source, a, body_start - 1)
    h = f"""    @staticmethod
    def _div_adjust_contradicts_pre_split_flag(df2, div_status_df, dt, div, div_adj_exceeds_prices):
{slc(source, find_line(source, "if div_adj_exceeds_prices:", a), mid - 1)}        return div_status_df

    @staticmethod
    def _div_adjust_contradicts_merge_flags(div_status_df, dt, div_status):
{before_return(source, mid, b)}        return div_status_df

"""
    body = """        div_adj_is_too_small = div_status_df.loc[dt, 'div_exceeds_adj']
        if div_adj_exceeds_prices and div_adj_is_too_small:
            div_adj_exceeds_prices = False
        div_status = {}
        div_status['adj_exceeds_prices'] = div_adj_exceeds_prices
        div_status['div_date_wrong'] = div_date_wrong
        div_status['div_true_date'] = div_true_date
        div_status_df = PriceHistory._div_adjust_contradicts_pre_split_flag(
            df2, div_status_df, dt, div, div_adj_exceeds_prices)
        return PriceHistory._div_adjust_contradicts_merge_flags(div_status_df, dt, div_status)
"""
    return replace_method_range(source, "_div_adjust_contradicts_apply_flags", h, sig + body)


def split_prepare_changes2(source: str) -> str:
    if "_sudden_change_changes_adj" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_prepare_changes")
    mid = find_line(source, "for j in range(price_data.shape[1]):", a)
    h = f"""    def _sudden_change_changes_adj(self, loc):
        df2 = loc['df2']
        df_workings = loc['df_workings']
        OHLC = loc['OHLC']
        price_data_cols = loc['price_data_cols']
        price_data = loc['price_data']
        f_zero = loc['f_zero']
        adj = loc['adj']
        df_dtype = loc['df_dtype']
{before_return(source, mid, b)}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, mid - 1)}        loc.update(locals())
        return self._sudden_change_changes_adj(loc)
"""
    return replace_method(source, "_sudden_change_prepare_changes", h, body)


def split_fp_spikes(source: str) -> str:
    if "_sudden_change_fp_spike_one" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_apply_fp_spikes")
    loop = find_line(source, "for idx in np.where(f_up_shifts)[0]:", a)
    loop_end = find_exact_line(source, "        loc.update(locals())", a)
    h = f"""    def _sudden_change_fp_spike_one(self, loc, idx):
        df2 = loc['df2']
        multiday = loc['multiday']
        logger = loc['logger']
        f_up = loc['f_up']
        f_up_ndims = loc['f_up_ndims']
        f_down = loc['f_down']
        nf_up_shifts = loc['nf_up_shifts']
        flat_indices = loc['flat_indices']
        down_dts = loc['down_dts']
{indent_method_body(slc(source, loop + 1, loop_end - 1))}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, loop - 1)}            for idx in np.where(f_up_shifts)[0]:
                loc = self._sudden_change_fp_spike_one(loc, idx)
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_sudden_change_apply_fp_spikes", h, body)


def split_vol_filter(source: str) -> str:
    if "_sudden_change_vol_filter_one" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_apply_vol_filter")
    loop = find_line(source, "for idx in np.where(f)[0]:", a)
    post = find_line(source, "if not correct_columns_individually:", a)
    h = f"""    def _sudden_change_vol_filter_one(self, loc, idx):
        df2 = loc['df2']
        interval = loc['interval']
        interday = loc['interday']
        logger = loc['logger']
        log_extras = loc['log_extras']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        price_data_cols = loc.get('price_data_cols', loc['OHLC'])
        split_max = loc['split_max']
        debug_cols = loc.get('debug_cols', ['Close'])
        f = loc['f']
{indent_method_body(slc(source, loop + 1, post - 1))}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, loop - 1)}        for idx in np.where(f)[0]:
            loc = self._sudden_change_vol_filter_one(loc, idx)
{slc(source, post, b - 1)}        loc.update(locals())
        return loc
"""
    return replace_method(source, "_sudden_change_apply_vol_filter", h, body)


def split_abort_check(source: str) -> str:
    if "_sudden_change_abort_split_near" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_apply_abort_check")
    mid = find_line(source, "if logger.isEnabledFor(logging.DEBUG):", a)
    h = f"""    def _sudden_change_abort_split_near(self, loc):
        df2 = loc['df2']
        df = loc['df']
        interval = loc['interval']
        change = loc['change']
        logger = loc['logger']
        log_extras = loc['log_extras']
        fix_type = loc['fix_type']
        f = loc['f']
{slc(source, find_line(source, "if not f.any():", a), mid - 1).replace("return df", "loc['_early_df'] = df")}        return loc

    def _sudden_change_abort_debug(self, loc):
        df_workings = loc['df_workings']
        correct_columns_individually = loc['correct_columns_individually']
        debug_cols = loc.get('debug_cols', ['Close'])
        logger = loc['logger']
{before_return(source, mid, b)}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, find_line(source, "if not f.any():", a) - 1)}        loc = self._sudden_change_abort_split_near(loc)
        if '_early_df' in loc:
            return loc['_early_df']
        loc = self._sudden_change_abort_debug(loc)
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_sudden_change_apply_abort_check", h, body)


def split_repair_individual(source: str) -> str:
    if "_sudden_change_repair_ind_ranges" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_repair_individual")
    loop = find_line(source, "for j in range(len(OHLC)):", a)
    count = find_line(source, "count = sum([1 if x is not None else 0 for x in OHLC_correct_ranges])", a)
    h = f"""    def _sudden_change_repair_ind_ranges(self, loc):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        interday = loc['interday']
        OHLC = loc['OHLC']
        n = loc['n']
        f = loc['f']
        f_up = loc['f_up']
        f_down = loc['f_down']
        start_min = loc.get('start_min')
        idx_latest_active = loc.get('idx_latest_active')
        appears_suspended = loc.get('appears_suspended')
        idx_rev_latest_active = loc.get('idx_rev_latest_active')
        OHLC_correct_ranges = [None, None, None, None]
{indent_method_body(slc(source, loop, count - 1))}        loc['OHLC_correct_ranges'] = OHLC_correct_ranges
        return loc

    def _sudden_change_repair_ind_apply(self, loc):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        interday = loc['interday']
        OHLC = loc['OHLC']
        n = loc['n']
        fix_type = loc['fix_type']
        split = loc['split']
        split_rcp = loc['split_rcp']
        correct_volume = loc['correct_volume']
        f_corrected = loc['f_corrected']
        f_open_fixed = loc.get('f_open_fixed')
        f_close_fixed = loc.get('f_close_fixed')
        OHLC_correct_ranges = loc['OHLC_correct_ranges']
{indent_method_body(before_return(source, count, b))}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, loop - 1)}        loc = self._sudden_change_repair_ind_ranges(loc)
        loc = self._sudden_change_repair_ind_apply(loc)
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_sudden_change_repair_individual", h, body)


def split_repair_combined(source: str) -> str:
    if "_sudden_change_repair_comb_ranges" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_repair_combined")
    ranges_loop = find_line(source, "for r in ranges:", a)
    n_corr = find_line(source, "n_corrected = 0", a)
    h = f"""    def _sudden_change_repair_comb_ranges(self, loc):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        n = loc['n']
        f = loc['f']
        f_up = loc['f_up']
        f_down = loc['f_down']
        start_min = loc.get('start_min')
        idx_latest_active = loc.get('idx_latest_active')
        appears_suspended = loc.get('appears_suspended')
        idx_rev_latest_active = loc.get('idx_rev_latest_active')
        split = loc['split']
{indent_method_body(slc(source, n_corr, ranges_loop - 1))}        loc.update(locals())
        return loc

    def _sudden_change_repair_comb_apply(self, loc):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        interday = loc['interday']
        fix_type = loc['fix_type']
        split = loc['split']
        split_rcp = loc['split_rcp']
        correct_dividend = loc['correct_dividend']
        correct_volume = loc['correct_volume']
        ranges = loc['ranges']
        n_corrected = loc.get('n_corrected', 0)
{indent_method_body(before_return(source, ranges_loop, b))}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, n_corr - 1)}        loc = self._sudden_change_repair_comb_ranges(loc)
        loc = self._sudden_change_repair_comb_apply(loc)
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_sudden_change_repair_combined", h, body)


def main():
    source = HISTORY.read_text(encoding="utf-8")
    steps = [
        fix_contradicts_apply,
        split_poss_data,
        split_scan_core,
        split_apply_flags,
        split_prepare_changes2,
        split_fp_spikes,
        split_vol_filter,
        split_abort_check,
        split_repair_individual,
        split_repair_combined,
    ]
    for step in steps:
        source = step(source)
        source = parse_ok(source, step.__name__)
    HISTORY.write_text(source, encoding="utf-8")
    r = subprocess.run(
        [sys.executable, "-m", "pylint", "yfinance/scrapers/history.py",
         "--disable=all", "--enable=R0902,R0915,R0912"],
        cwd=ROOT, capture_output=True, text=True,
    )
    print(r.stdout + r.stderr)
    if r.returncode != 0:
        sys.exit(r.returncode)
    print("phase14 ok")


if __name__ == "__main__":
    main()
