"""Phase 10: final pylint cleanups on already-refactored history.py."""
from __future__ import annotations

import ast
from pathlib import Path

import refactor_history_phase7 as p7

HISTORY = Path(__file__).resolve().parent.parent / "yfinance" / "scrapers" / "history.py"
slc = p7.slc
find_line = p7.find_line
replace_method = p7.replace_method
method_range = p7.method_range
dedent_block = p7.dedent_block


def fix_apply_rows(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_apply_rows")
    body = slc(source, a + 1, b - 1)
    if "no_fine_data_dts = []" not in body:
        body = body.replace(
            "bad_dts = loc['bad_dts']\n",
            "bad_dts = loc['bad_dts']\n        no_fine_data_dts = []\n",
            1,
        )
    return replace_method(source, "_reconstruct_repair_apply_rows", "", body)


def split_fetch_request(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_fetch_request")
    mid = find_line(source, "td_1d = _datetime.timedelta(days=1)", a)
    h1 = f"""    @staticmethod
    def _reconstruct_fetch_window(g, interval, intraday, td_range, min_dt, start_d):
{slc(source, mid, b - 2)}        return fetch_start, fetch_end, df_fine, log_level, logger

"""
    body = f"""{slc(source, a + 1, mid - 1)}        fetch_start, fetch_end, df_fine, log_level, logger = (
            PriceHistory._reconstruct_fetch_window(
                g, interval, intraday, td_range, min_dt, start_d))
        if df_fine is None or df_fine.empty:
            msg = f"Cannot reconstruct block starting {{start_dt if intraday else start_d}}, too old, Yahoo will reject request for finer-grain data"
            logger.info(msg, extra=log_extras)
            loc['_early'] = True; return loc
        loc.update(locals())
        return loc
"""
    return replace_method(source, "_reconstruct_repair_fetch_request", h1, body)


def split_calibrate_ratio(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_calibrate_ratio")
    mid = find_line(source, "if abs(ratio/0.0001 -1) < 0.01:", a)
    h1 = f"""    @staticmethod
    def _reconstruct_compute_ratio(df_block, df_new, df_fine_grp, common_index, tag, calib_cols):
{slc(source, a + 1, mid - 1)}        return ratio, ratio_rcp, df_block, df_new, df_v2

"""
    body = f"""        ratio, ratio_rcp, df_block, df_new, df_v2 = PriceHistory._reconstruct_compute_ratio(
            df_block, df_new, df_fine_grp, common_index, tag, ['Open', 'Close'])
        df = loc['df']
        interval = loc['interval']
        price_cols = loc['price_cols']
        start_d = loc['start_d']
        logger = loc['logger']
        log_extras = loc['log_extras']
{slc(source, mid, b - 2)}        loc.update(locals())
        return loc
"""
    return replace_method(source, "_reconstruct_repair_calibrate_ratio", h1, body)


def split_apply_row(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_apply_rows")
    loop = find_line(source, "for idx in bad_dts:", a)
    inner = find_line(source, "df_bad_row = df.loc[idx]", loop)
    h1 = f"""    @staticmethod
    def _reconstruct_repair_one_row(df, df_v2, df_new, df_fine, interval, tag, idx):
{dedent_block(slc(source, inner, b - 2), 8)}        return df_v2, df_fine

"""
    body = f"""{slc(source, a + 1, inner - 1)}            df_v2, df_fine = PriceHistory._reconstruct_repair_one_row(
                df, df_v2, df_new, df_fine, interval, tag, idx)
        return df_v2
"""
    return replace_method(source, "_reconstruct_repair_apply_rows", h1, body)


def main():
    source = HISTORY.read_text(encoding="utf-8")
    source = fix_apply_rows(source)
    ast.parse(source)
    HISTORY.write_text(source, encoding="utf-8")
    print("phase10 ok")


if __name__ == "__main__":
    main()
