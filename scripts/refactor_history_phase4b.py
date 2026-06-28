"""Phase 4b: further split reconstruct helpers."""
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
    group_size = slice_abs(source, 756, 767)
    group_rest = slice_abs(source, 769, 796)
    repair_one = slice_abs(source, 816, 1066)

    helpers = f"""    def _reconstruct_init_context(self, df, interval, prepost, tag):
        logger = utils.get_yf_logger()
        log_extras = {{'yf_cat': 'price-reconstruct', 'yf_interval': interval, 'yf_symbol': self.ticker}}
{slice_abs(source, 663, 687)}        return df, interval, prepost, logger, log_extras, intraday, price_cols, data_cols, itds, nexts, min_lookbacks

    def _reconstruct_select_subinterval(self, df, interval, nexts, logger, log_extras):
{slice_abs(source, 688, 703)}        return df, sub_interval, td_range

    def _reconstruct_collect_repair_rows(self, df, data_cols, tag, sub_interval, min_lookbacks, logger, log_extras):
{slice_abs(source, 705, 742)}        return df, dts_to_repair, df_v2, df_good

    @staticmethod
    def _reconstruct_grp_max_size(sub_interval):
{group_size}        return grp_max_size

    def _reconstruct_repair_one_group(self, g, loc, n_fixed):
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
{repair_one}        return df_v2, n_fixed

"""

    setup_body = """        ctx = self._reconstruct_init_context(df, interval, prepost, tag)
        if isinstance(ctx, pd.DataFrame):
            return ctx
        df, interval, prepost, logger, log_extras, intraday, price_cols, data_cols, itds, nexts, min_lookbacks = ctx
        sub = self._reconstruct_select_subinterval(df, interval, nexts, logger, log_extras)
        if isinstance(sub, pd.DataFrame):
            return sub
        df, sub_interval, td_range = sub
        rows = self._reconstruct_collect_repair_rows(df, data_cols, tag, sub_interval, min_lookbacks, logger, log_extras)
        if isinstance(rows, pd.DataFrame):
            return rows
        df, dts_to_repair, df_v2, df_good = rows
        return locals()
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
{group_rest}        return dts_groups
"""

    repair_body = """        for g in dts_groups:
            loc['df_v2'], _ = self._reconstruct_repair_one_group(g, loc, 0)
        return loc['df_v2']
"""

    source = replace_method(source, "_reconstruct_setup", helpers, setup_body)
    source = replace_method(source, "_reconstruct_build_groups", "", groups_body)
    source = replace_method(source, "_reconstruct_repair_groups", "", repair_body)

    HISTORY.write_text(source, encoding="utf-8")
    print("Phase 4b done")


if __name__ == "__main__":
    main()
