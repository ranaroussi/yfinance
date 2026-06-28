"""Phase 15: final pylint cleanup on history.py."""
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


def fix_apply_rows_return(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_apply_rows")
    if "return df_v2" in slc(source, a, b):
        return source
    body = slc(source, a + 1, b - 1)
    if "n_fixed = 0" not in body:
        body = body.replace(
            "bad_dts = loc['bad_dts']",
            "bad_dts = loc['bad_dts']\n        n_fixed = 0",
            1,
        )
    body = body.rstrip() + "\n        return df_v2\n"
    sig = lines(source)[a - 1]
    while not sig.strip().endswith(":"):
        a -= 1
        sig = lines(source)[a - 1] + sig
    return replace_method_range(source, "_reconstruct_repair_apply_rows", "", sig + body)


def dedupe_apply_main(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_detect_apply_main")
    first = find_line(source, "        df2 = loc['df2']", a)
    try:
        dup = find_line(source, "        df2 = loc['df2']", first + 1)
    except ValueError:
        return source
    body = slc(source, a + 1, dup - 1) + slc(source, find_line(source, "loc = self._sudden_change_apply_fp_spikes", a), b - 1)
    sig = slc(source, a, a)
    return replace_method_range(source, "_sudden_change_detect_apply_main", "", sig + body)


def dedupe_prepare_changes(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_prepare_changes")
    first = find_line(source, "        df2 = loc['df2']", a)
    try:
        dup = find_line(source, "        df2 = loc['df2']", first + 1)
    except ValueError:
        return source
    try:
        tail_start = find_line(source, "loc = self._sudden_change_changes_price_matrix", a)
    except ValueError:
        try:
            tail_start = find_line(source, "# Calculate daily price % change", a)
        except ValueError:
            return source
    body = slc(source, a + 1, dup - 1) + slc(source, tail_start, b - 1)
    sig = slc(source, a, a)
    return replace_method_range(source, "_sudden_change_prepare_changes", "", sig + body)


def split_fetch_request(source: str) -> str:
    if "_reconstruct_repair_fetch_window" in source:
        return source
    a, b, _ = method_range(source, "_reconstruct_repair_fetch_request")
    mid = find_line(source, "# The first and last day returned", a)
    h = f"""    def _reconstruct_repair_fetch_window(self, g, loc):
        interval = loc['interval']
        intraday = loc['intraday']
        td_range = loc['td_range']
        min_dt = loc['min_dt']
        start_dt = g[0]
        start_d = start_dt.date()
{slc(source, mid, find_line(source, "logger.debug(f\"Fetching", a) - 1)}        loc.update(locals())
        return loc

    def _reconstruct_repair_fetch_call(self, g, loc):
        prepost = loc['prepost']
        sub_interval = loc['sub_interval']
        intraday = loc['intraday']
        logger = loc['logger']
        log_extras = loc['log_extras']
        start_dt = g[0]
        start_d = start_dt.date()
        fetch_start = loc['fetch_start']
        fetch_end = loc['fetch_end']
{before_return(source, find_line(source, "logger.debug(f\"Fetching", a), b)}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, mid - 1)}        loc = self._reconstruct_repair_fetch_window(g, loc)
        if loc.get('_early'):
            return loc
        loc = self._reconstruct_repair_fetch_call(g, loc)
        return loc
"""
    sig = slc(source, a, a)
    return replace_method_range(source, "_reconstruct_repair_fetch_request", h, sig + body)


def before_return(source: str, start: int, end: int) -> str:
    ret = end
    while ret >= start:
        if lines(source)[ret - 1].strip().startswith("return"):
            return slc(source, start, ret - 1)
        ret -= 1
    return slc(source, start, end - 1)


def split_calibrate(source: str) -> str:
    if "_reconstruct_repair_calibrate_compute" in source:
        return source
    a, b, _ = method_range(source, "_reconstruct_repair_calibrate_ratio")
    mid = find_line(source, "if abs(ratio/0.0001 -1)", a)
    h = f"""    def _reconstruct_repair_calibrate_compute(self, loc):
        df_block = loc['df_block']
        df_new = loc['df_new']
        df_fine_grp = loc['df_fine_grp']
        tag = loc['tag']
        calib_cols = ['Open', 'Close']
{slc(source, find_line(source, "calib_cols = ", a), mid - 1)}        loc.update(locals())
        return loc

    def _reconstruct_repair_calibrate_apply(self, loc):
        df_new = loc['df_new']
        df_v2 = loc['df_v2']
        price_cols = loc['price_cols']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        ratio = loc['ratio']
        ratio_rcp = loc['ratio_rcp']
{before_return(source, mid, b)}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, find_line(source, "calib_cols = ", a) - 1)}        loc = self._reconstruct_repair_calibrate_compute(loc)
        if loc.get('_early'):
            return loc
        return self._reconstruct_repair_calibrate_apply(loc)
"""
    sig = slc(source, a, a)
    return replace_method_range(source, "_reconstruct_repair_calibrate_ratio", h, sig + body)


def split_apply_rows(source: str) -> str:
    if "_reconstruct_repair_apply_one_row" in source:
        return source
    a, b, _ = method_range(source, "_reconstruct_repair_apply_rows")
    first_loop = find_line(source, "for idx in bad_dts:", a)
    loop = find_line(source, "for idx in bad_dts:", first_loop + 1)
    inner_start = find_line(source, "df_new_row = df_new.loc[idx]", loop)
    inner_end = find_line(source, "n_fixed += 1", loop)
    h = f"""    def _reconstruct_repair_apply_one_row(self, loc, idx):
        df = loc['df']
        df_v2 = loc['df_v2']
        df_new = loc['df_new']
        df_fine = loc['df_fine']
        interval = loc['interval']
        tag = loc['tag']
{indent_method_body(slc(source, inner_start, inner_end))}        loc['n_fixed'] = loc.get('n_fixed', 0) + 1
        return loc

"""
    body = f"""{slc(source, a + 1, loop - 1)}        n_fixed = 0
        for idx in bad_dts:
            if idx not in df_new.index:
                return df_v2
            loc = self._reconstruct_repair_apply_one_row(loc, idx)
        return df_v2
"""
    return replace_method(source, "_reconstruct_repair_apply_rows", h, body)


def split_prepare_changes2(source: str) -> str:
    if "_sudden_change_changes_price_matrix" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_prepare_changes")
    start = find_line(source, "# Calculate daily price % change", a)
    end = find_line(source, "loc.update(locals())", a)
    h = f"""    def _sudden_change_changes_price_matrix(self, loc):
        df2 = loc['df2']
        interval = loc['interval']
        split = loc['split']
        interday = loc['interday']
        OHLC = loc['OHLC']
        n = loc['n']
{before_return(source, start, end)}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, start - 1)}        loc = self._sudden_change_changes_price_matrix(loc)
        loc = self._sudden_change_changes_adj(loc)
        return self._sudden_change_changes_iqr(loc)
"""
    sig = slc(source, a, a)
    return replace_method_range(source, "_sudden_change_prepare_changes", h, sig + body)


def split_vol_filter_col(source: str) -> str:
    if "_sudden_change_vol_filter_col" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_vol_filter_one")
    loop = find_line(source, "for c in cols:", a)
    inner_end = find_line(source, "loc.update(locals())", loop) - 1
    h = f"""    def _sudden_change_vol_filter_col(self, loc, idx, c):
        df2 = loc['df2']
        interval = loc['interval']
        interday = loc['interday']
        logger = loc['logger']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        split_max = loc['split_max']
        dt = df2.index[idx]
        changes_local = loc['changes_local']
{indent_method_body(slc(source, loop + 1, inner_end))}        loc.update(locals())
        return loc

"""
    tail = slc(source, find_line(source, "loc.update(locals())", loop), b - 1)
    body = f"""{slc(source, a + 1, loop - 1)}        loc['changes_local'] = changes_local
        for c in cols:
            loc = self._sudden_change_vol_filter_col(loc, idx, c)
{tail}"""
    return replace_method(source, "_sudden_change_vol_filter_one", h, body)


def split_repair_ind_correct(source: str) -> str:
    if "_sudden_change_repair_ind_correct" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_repair_ind_apply")
    else_b = find_exact_line(source, "        else:", find_line(source, "elif count == 1:", a))
    vol_if = find_exact_line(
        source,
        "        if correct_volume:",
        find_line(source, "logger.info(msg, extra=log_extras)", else_b),
    )
    h = f"""    def _sudden_change_repair_ind_correct(self, loc):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        interday = loc['interday']
        OHLC = loc['OHLC']
        fix_type = loc['fix_type']
        split = loc['split']
        split_rcp = loc['split_rcp']
        correct_volume = loc['correct_volume']
        f_corrected = loc['f_corrected']
        f_open_fixed = loc.get('f_open_fixed')
        f_close_fixed = loc.get('f_close_fixed')
        OHLC_correct_ranges = loc['OHLC_correct_ranges']
{indent_method_body(slc(source, else_b + 1, vol_if - 1))}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, else_b - 1)}        else:
            loc = self._sudden_change_repair_ind_correct(loc)
{slc(source, vol_if, b - 1)}"""
    return replace_method(source, "_sudden_change_repair_ind_apply", h, body)


def split_repair_comb_range(source: str) -> str:
    if "_sudden_change_repair_comb_one_range" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_repair_comb_apply")
    loop = find_line(source, "for r in ranges:", a)
    after_loop = find_line(source, "if len(ranges) <= 2:", loop)
    h = f"""    def _sudden_change_repair_comb_one_range(self, loc, r):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        interday = loc['interday']
        fix_type = loc['fix_type']
        split = loc['split']
        split_rcp = loc['split_rcp']
        correct_dividend = loc['correct_dividend']
        correct_volume = loc['correct_volume']
        n_corrected = loc.get('n_corrected', 0)
{indent_method_body(slc(source, loop + 1, after_loop - 1))}        loc['n_corrected'] = n_corrected
        return loc

"""
    body = f"""{slc(source, a + 1, loop - 1)}        for r in ranges:
            loc = self._sudden_change_repair_comb_one_range(loc, r)
{slc(source, after_loop, b - 1)}"""
    return replace_method(source, "_sudden_change_repair_comb_apply", h, body)


def split_repair_ind_one_range(source: str) -> str:
    if "_sudden_change_repair_ind_one_range" in source:
        return source
    a, b, _ = method_range(source, "_sudden_change_repair_ind_correct")
    loop = find_line(source, "for r in ranges:", a)
    inner_end = find_line(source, "if sum(n_corrected) > 0:", loop) - 1
    log_start = find_line(source, "if sum(n_corrected) > 0:", a)
    log_end = find_line(source, "loc.update(locals())", log_start)
    h = f"""    def _sudden_change_repair_ind_one_range(self, loc, j, c, r):
        df2 = loc['df2']
        logger = loc['logger']
        log_extras = loc['log_extras']
        interday = loc['interday']
        fix_type = loc['fix_type']
        split = loc['split']
        split_rcp = loc['split_rcp']
        correct_volume = loc['correct_volume']
        f_corrected = loc['f_corrected']
        f_open_fixed = loc.get('f_open_fixed')
        f_close_fixed = loc.get('f_close_fixed')
        n_corrected = loc['n_corrected']
{indent_method_body(slc(source, loop + 1, inner_end))}        loc['n_corrected'] = n_corrected
        return loc

    def _sudden_change_repair_ind_log_counts(self, loc):
        logger = loc['logger']
        log_extras = loc['log_extras']
        OHLC = loc['OHLC']
        n_corrected = loc['n_corrected']
{indent_method_body(slc(source, log_start, log_end - 1))}        loc.update(locals())
        return loc

"""
    body = f"""{slc(source, a + 1, loop - 1)}        loc['n_corrected'] = [0, 0, 0, 0]
        for j in range(len(OHLC)):
            c = OHLC[j]
            ranges = OHLC_correct_ranges[j]
            if ranges is None:
                ranges = []
            for r in ranges:
                loc = self._sudden_change_repair_ind_one_range(loc, j, c, r)
        return self._sudden_change_repair_ind_log_counts(loc)
"""
    return replace_method(source, "_sudden_change_repair_ind_correct", h, body)


def main():
    source = HISTORY.read_text(encoding="utf-8")
    steps = [
        fix_apply_rows_return,
        dedupe_apply_main,
        split_fetch_request,
        split_calibrate,
        split_apply_rows,
        split_prepare_changes2,
        dedupe_prepare_changes,
        split_vol_filter_col,
        split_repair_ind_correct,
        split_repair_comb_range,
        split_repair_ind_one_range,
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
    print("phase15 ok")


if __name__ == "__main__":
    main()
