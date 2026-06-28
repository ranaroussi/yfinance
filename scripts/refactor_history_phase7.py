"""Phase 7: fix refactor bugs and split remaining pylint smells."""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"


def method_range(source: str, name: str) -> tuple[int, int, ast.FunctionDef]:
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return fn.lineno, fn.end_lineno, fn


def lines(source: str) -> list[str]:
    return source.splitlines(keepends=True)


def slc(source: str, a: int, b: int) -> str:
    return "".join(lines(source)[a - 1 : b])


def find_line(source: str, pattern: str, start: int = 1) -> int:
    for i, line in enumerate(lines(source)[start - 1 :], start):
        if pattern in line:
            return i
    raise ValueError(f"pattern not found: {pattern!r} from line {start}")


def replace_method(source: str, name: str, prefix: str, body: str) -> str:
    a, b, _ = method_range(source, name)
    ls = lines(source)
    header = ls[a - 1]
    i = a
    while i < b:
        line = ls[i]
        stripped = line.strip()
        if stripped.startswith(('"""', "'''", "@")):
            header += line
            i += 1
        elif stripped and not line.startswith("        "):
            header += line
            i += 1
        else:
            break
    return "".join(ls[: a - 1]) + prefix + header + body + "\n" + "".join(ls[b:])


def insert_before_method(source: str, before: str, text: str) -> str:
    a, _, _ = method_range(source, before)
    ls = lines(source)
    return "".join(ls[: a - 1]) + text + "".join(ls[a - 1 :])


def assigned_in_function(fn: ast.FunctionDef) -> list[str]:
    names: set[str] = set()
    for node in fn.body:
        if isinstance(node, ast.FunctionDef):
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Store):
                names.add(sub.id)
    return sorted(names)


def dedent_block(text: str, spaces: int = 4) -> str:
    prefix = " " * spaces
    out = []
    for line in text.splitlines(keepends=True):
        if line.startswith(prefix):
            out.append(line[spaces:])
        else:
            out.append(line)
    return "".join(out)


def indent_block(text: str, spaces: int = 4) -> str:
    prefix = " " * spaces
    return "".join(prefix + line if line.strip() else line for line in text.splitlines(keepends=True))


def extract_cluster_dividends(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_analyse_dividends")
    cluster_start = find_line(source, "def cluster_dividends", a)
    if cluster_start > b:
        return source
    cluster_end = cluster_start
    while cluster_end < b and not (
        lines(source)[cluster_end - 1].strip() == "return cluster_labels"
        or "return cluster_labels" in lines(source)[cluster_end - 1]
    ):
        cluster_end += 1
    cluster_body = slc(source, cluster_start, cluster_end)
    static = cluster_body.replace(
        "        def cluster_dividends", "    @staticmethod\n    def _cluster_dividends", 1
    )
    ls = lines(source)
    without = "".join(ls[: cluster_start - 1]) + "".join(ls[cluster_end:])
    without = without.replace("cluster_dividends(", "PriceHistory._cluster_dividends(")
    a2, b2, _ = method_range(without, "_div_adjust_analyse_dividends")
    bad = find_line(without, "if div_status_df is None and not df_modified", a2)
    ls2 = lines(without)
    fixed = "".join(ls2[: bad - 1]) + "        return div_status_df, df2\n" + "".join(ls2[b2:])
    a3, _, _ = method_range(fixed, "_div_adjust_fix_pre_div_close")
    return "".join(lines(fixed)[: a3 - 1]) + static + "\n" + "".join(lines(fixed)[a3 - 1 :])


def fix_cluster_reconcile(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_cluster_reconcile")
    bad = find_line(source, "if div_status_df.empty:", a)
    ls = lines(source)
    end_bad = bad
    while end_bad < b and "return df2" not in lines(source)[end_bad - 1]:
        end_bad += 1
    return "".join(ls[: bad - 1]) + "".join(ls[end_bad:])


def fix_apply_repairs(source: str) -> str:
    return source.replace("        return df2\n        return df2\n", "        return df2\n", 1)


def fix_sudden_change_detect(source: str) -> str:
    det_a, _, _ = method_range(source, "_sudden_change_detect_and_fix")
    mid = find_line(source, "# Now can detect bad split adjustments", det_a)
    fp_line = find_line(source, "def _fix_prices_sudden_change", mid)
    fix_a, fix_b, _ = method_range(source, "_fix_prices_sudden_change")
    detect = indent_block(slc(source, mid, fp_line - 1), 4)
    dlines = detect.splitlines(keepends=True)
    mid1 = next(
        i for i, l in enumerate(dlines)
        if "if correct_columns_individually:" in l and "for j" not in l
    )
    mid2 = next(i for i, l in enumerate(dlines) if "f_up = _1d_change_x > threshold" in l)
    part_a = "".join(dlines[:mid1])
    part_b = "".join(dlines[mid1:mid2])
    part_c = "".join(dlines[mid2:])

    helpers = f"""    def _sudden_change_detect_threshold(self, loc):
        df2 = loc['df2']
        change = loc['change']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        _1d_change_denoised = loc['_1d_change_denoised']
        largest_change_pct = loc['largest_change_pct']
{part_a}        loc.update(locals())
        return loc

    def _sudden_change_detect_signals(self, loc):
        df2 = loc['df2']
        interval = loc['interval']
        change = loc['change']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        multiday = loc['multiday']
        OHLC = loc['OHLC']
        n = loc['n']
        _1d_change_denoised = loc['_1d_change_denoised']
        largest_change_pct = loc['largest_change_pct']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        threshold = loc['threshold']
        split_max = loc['split_max']
{part_b}        loc.update(locals())
        return loc

    def _sudden_change_detect_apply(self, loc):
        df2 = loc['df2']
        interval = loc['interval']
        tz_exchange = loc['tz_exchange']
        change = loc['change']
        correct_volume = loc['correct_volume']
        correct_dividend = loc['correct_dividend']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        multiday = loc['multiday']
        interday = loc['interday']
        OHLC = loc['OHLC']
        n = loc['n']
        fix_type = loc['fix_type']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        _1d_change_denoised = loc['_1d_change_denoised']
        _1d_change_x = loc['_1d_change_x']
        price_data_cols = loc.get('price_data_cols', OHLC)
        threshold = loc['threshold']
        split_max = loc['split_max']
        r = loc['r']
        f_down = loc['f_down']
        f_up = loc['f_up']
        f_up_ndims = loc['f_up_ndims']
        f_up_shifts = loc['f_up_shifts']
        start_min = loc.get('start_min')
        df = loc['df']
{part_c}        return df2

    def _sudden_change_detect_and_fix(self, df, loc):
        loc = self._sudden_change_detect_threshold(loc)
        loc = self._sudden_change_detect_signals(loc)
        return self._sudden_change_detect_apply(loc)

"""
    ls = lines(source)
    return "".join(ls[: det_a - 1]) + helpers + slc(source, fix_a, fix_b) + "\n" + "".join(ls[fix_b:])


def split_sudden_change_prepare(source: str) -> str:
    a, b, _ = method_range(source, "_sudden_change_prepare")
    mid = find_line(source, "# Calculate daily price % change", a)
    helpers = f"""    def _sudden_change_prepare_setup(self, df, interval, tz_exchange, change):
{slc(source, a + 1, mid - 1)}        return locals()

    def _sudden_change_prepare_changes(self, loc):
        df2 = loc['df2']
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
{slc(source, mid, b - 1)}        loc.update(locals())
        return loc

"""
    body = """        if df.empty:
            return df
        loc = self._sudden_change_prepare_setup(df, interval, tz_exchange, change)
        if isinstance(loc, pd.DataFrame):
            return loc
        loc['df'] = df
        return self._sudden_change_prepare_changes(loc)
"""
    return replace_method(source, "_sudden_change_prepare", helpers, body)


def split_reconstruct_repair_one(source: str) -> str:
    a, b, _ = method_range(source, "_reconstruct_repair_one_group")
    cal = find_line(source, "# Calibrate!", a)
    rep = find_line(source, "# Repair!", a)
    fetch_start = find_line(source, "df_block = df", a)
    fetch_mid = find_line(source, "# Discard the buffer", a)
    cal_mid = find_line(source, "# Check whether 'df_fine'", a)
    fetch_a = slc(source, fetch_start, fetch_mid - 1).replace(
        "return df_v2", "loc['_early'] = True; return loc"
    )
    fetch_b = slc(source, fetch_mid, cal - 1).replace(
        "return df_v2", "loc['_early'] = True; return loc"
    )
    calib_a = slc(source, cal, cal_mid - 1).replace(
        "return df_v2", "loc['_early'] = True; return loc"
    )
    calib_b = slc(source, cal_mid, rep - 1).replace(
        "return df_v2", "loc['_early'] = True; return loc"
    )
    repair = slc(source, rep, b - 2)

    helpers = f"""    def _reconstruct_repair_fetch_request(self, g, loc):
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
{fetch_a}        loc.update(locals())
        return loc

    def _reconstruct_repair_fetch_aggregate(self, g, loc):
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
        df_block = loc['df_block']
        start_dt = loc['start_dt']
        start_d = loc['start_d']
{fetch_b}        loc.update(locals())
        return loc

    def _reconstruct_repair_calibrate_adj(self, loc):
        df = loc['df']
        df_v2 = loc['df_v2']
        df_block = loc['df_block']
        df_new = loc['df_new']
        df_fine = loc['df_fine']
        df_fine_grp = loc['df_fine_grp']
        interval = loc['interval']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        price_cols = loc['price_cols']
        start_d = loc['start_d']
{calib_a}        loc.update(locals())
        return loc

    def _reconstruct_repair_calibrate_ratio(self, loc):
        df = loc['df']
        df_v2 = loc['df_v2']
        df_block = loc['df_block']
        df_new = loc['df_new']
        df_fine = loc['df_fine']
        df_fine_grp = loc['df_fine_grp']
        interval = loc['interval']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        price_cols = loc['price_cols']
        start_d = loc['start_d']
{calib_b}        loc.update(locals())
        return loc

    def _reconstruct_repair_apply(self, loc):
        df = loc['df']
        df_v2 = loc['df_v2']
        df_block = loc['df_block']
        df_new = loc['df_new']
        df_fine = loc['df_fine']
        interval = loc['interval']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        price_cols = loc['price_cols']
{repair}        return df_v2

"""
    header = slc(source, a + 1, a + 16)
    body = header + """        loc = self._reconstruct_repair_fetch_request(g, loc)
        if loc.get('_early'):
            return loc['df_v2']
        loc = self._reconstruct_repair_fetch_aggregate(g, loc)
        if loc.get('_early'):
            return loc['df_v2']
        loc = self._reconstruct_repair_calibrate_adj(loc)
        if loc.get('_early'):
            return loc['df_v2']
        loc = self._reconstruct_repair_calibrate_ratio(loc)
        if loc.get('_early'):
            return loc['df_v2']
        return self._reconstruct_repair_apply(loc)
"""
    return replace_method(source, "_reconstruct_repair_one_group", helpers, body)


def split_div_analyse(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_analyse_dividends")
    loop_start = find_line(source, "for i in range(len(div_indices)-1, -1, -1):", a)
    loop_body = dedent_block(slc(source, loop_start + 1, b - 2))
    helper = f"""    @staticmethod
    def _div_adjust_analyse_one(df2, div_indices, i, currency_divide, too_big_check_threshold, div_status_df):
{loop_body}        return div_status_df, df2

"""
    body = """        div_status_df = None
        for i in range(len(div_indices)-1, -1, -1):
            div_status_df, df2 = PriceHistory._div_adjust_analyse_one(
                df2, div_indices, i, currency_divide, too_big_check_threshold, div_status_df)
        return div_status_df, df2
"""
    return replace_method(source, "_div_adjust_analyse_dividends", helper, body)


def split_div_mark_phantoms(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_mark_phantoms")
    mid = find_line(source, "# There might be other phantom dividends", a)
    h1 = f"""    @staticmethod
    def _div_adjust_mark_phantoms_first(div_status_df, checks, currency_divide):
{slc(source, a + 1, mid - 1)}        return div_status_df, checks

"""
    body = f"""        div_status_df, checks = PriceHistory._div_adjust_mark_phantoms_first(
            div_status_df, checks, currency_divide)
{slc(source, mid, b - 2)}        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_mark_phantoms", h1, body)


def split_div_contradicts(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_contradicts_prices")
    loop_start = find_line(source, "for i in range(len(div_status_df)):", a)
    prune = find_line(source, "# Can prune the space:", a)
    loop_body = dedent_block(slc(source, loop_start + 1, prune - 1))
    tail = slc(source, prune, b - 2)
    helper = f"""    @staticmethod
    def _div_adjust_contradicts_one(df2, div_status_df, i, checks):
{loop_body}        return div_status_df

"""
    body = f"""        for i in range(len(div_status_df)):
            div_status_df = PriceHistory._div_adjust_contradicts_one(df2, div_status_df, i, checks)
{tail}        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_contradicts_prices", helper, body)


def split_div_cluster(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_cluster_reconcile")
    mid = find_line(source, "if 'div_too_big' in checks and 'div_exceeds_adj' in checks:", a)
    h1 = f"""    @staticmethod
    def _div_adjust_cluster_inconsistencies(div_status_df, checks, logger, log_extras):
{slc(source, a + 1, mid - 1)}        return div_status_df, checks

"""
    body = f"""        div_status_df, checks = PriceHistory._div_adjust_cluster_inconsistencies(
            div_status_df, checks, logger, log_extras)
{slc(source, mid, b - 2)}        return div_status_df, checks
"""
    return replace_method(source, "_div_adjust_cluster_reconcile", h1, body)


def split_div_apply(source: str) -> str:
    a, b, _ = method_range(source, "_div_adjust_apply_repairs")
    n1 = find_line(source, "if n_failed_checks == 1:", a)
    n2 = find_line(source, "elif n_failed_checks == 2:", a)
    n3 = find_line(source, "elif n_failed_checks == 3:", a)
    loop_end = find_line(source, "if cluster.empty:", a)
    helpers = f"""    @staticmethod
    def _div_adjust_repair_one(
            df2, df2_nan, div_status_df, cluster, row, dt, enddt, checks, currency_divide, div_repairs,
            adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
            div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small):
{dedent_block(slc(source, n1 + 1, n2 - 1), 12)}        return df2, df2_nan, div_status_df, cluster, div_repairs

    @staticmethod
    def _div_adjust_repair_two(
            df2, df2_nan, cluster, row, dt, enddt, currency_divide, div_repairs,
            div_too_small, div_too_big, div_exceeds_adj, adj_exceeds_prices, adj_missing, adj_exceeds_div):
{dedent_block(slc(source, n2 + 1, n3 - 1), 12)}        return df2, df2_nan, cluster, div_repairs

    @staticmethod
    def _div_adjust_repair_three(
            df2, df2_nan, cluster, row, dt, enddt, currency_divide, div_repairs,
            div_too_big, div_exceeds_adj, div_pre_split, adj_too_small):
{dedent_block(slc(source, n3 + 1, loop_end - 1), 12)}        return df2, df2_nan, cluster, div_repairs

"""
    wrapper = slc(source, a + 1, n1 - 1)
    call_one = """                if n_failed_checks == 1:
                    df2, df2_nan, div_status_df, cluster, div_repairs = PriceHistory._div_adjust_repair_one(
                        df2, df2_nan, div_status_df, cluster, row, dt, enddt, checks, currency_divide, div_repairs,
                        adj_missing, div_exceeds_adj, adj_exceeds_div, adj_exceeds_prices,
                        div_too_small, div_too_big, div_pre_split, div_date_wrong, adj_too_small)
                elif n_failed_checks == 2:
                    df2, df2_nan, cluster, div_repairs = PriceHistory._div_adjust_repair_two(
                        df2, df2_nan, cluster, row, dt, enddt, currency_divide, div_repairs,
                        div_too_small, div_too_big, div_exceeds_adj, adj_exceeds_prices, adj_missing, adj_exceeds_div)
                elif n_failed_checks == 3:
                    df2, df2_nan, cluster, div_repairs = PriceHistory._div_adjust_repair_three(
                        df2, df2_nan, cluster, row, dt, enddt, currency_divide, div_repairs,
                        div_too_big, div_exceeds_adj, div_pre_split, adj_too_small)
"""
    tail = slc(source, loop_end, b - 2)
    body = wrapper + call_one + tail + "        return df2\n"
    return replace_method(source, "_div_adjust_apply_repairs", helpers, body)


def split_fix_bad_div_init(source: str) -> str:
    a, b, _ = method_range(source, "_fix_bad_div_adjust")
    fix_line = find_line(source, "df2, df_modified = self._div_adjust_fix_pre_div_close", a)
    helper = f"""    def _div_adjust_init(self, df, interval, currency, logger, log_extras):
{slc(source, a + 1, fix_line - 1)}        return df, interval, currency, logger, log_extras, currency_divide, div_status_df, too_big_check_threshold, df2, df2_nan, df_modified, div_indices

"""
    body = """        if df is None or df.empty:
            return df
        if interval in ['1wk', '1mo', '3mo', '1y']:
            return df
        if 'Capital Gains' in df.columns and (df['Capital Gains']>0).any():
            return df
        init = self._div_adjust_init(df, interval, currency, utils.get_yf_logger(), {
            'yf_cat': 'div-adjust-repair-bad', 'yf_interval': interval, 'yf_symbol': self.ticker})
        if isinstance(init, pd.DataFrame):
            return init
        (df, interval, currency, logger, log_extras, currency_divide, div_status_df,
         too_big_check_threshold, df2, df2_nan, df_modified, div_indices) = init
"""
    rest = slc(source, fix_line, b - 2)
    return replace_method(source, "_fix_bad_div_adjust", helper, body + rest)


HISTORY_EXEC_GLOBALS = (
    "'pd': pd, 'np': np, 'utils': utils, '_datetime': _datetime, '_time': _time, "
    "'warnings': warnings, 'logging': logging, 'YfConfig': YfConfig, "
    "'YFTzMissingError': YFTzMissingError, 'YFPricesMissingError': YFPricesMissingError, "
    "'YFInvalidPeriodError': YFInvalidPeriodError, 'YFRateLimitError': YFRateLimitError, "
    "'YFDataException': YFDataException, '_BASE_URL_': _BASE_URL_, "
    "'_PRICE_COLNAMES_': _PRICE_COLNAMES_, 'period_default': period_default"
)


def split_history_class(source: str) -> str:
    a, b, fn = method_range(source, "history")
    body_start = find_line(source, "logger = utils.get_yf_logger()", a)
    prepare_start = find_line(source, "interval_user = interval", a)
    header = slc(source, a, body_start - 1)
    prefix = slc(source, body_start, prepare_start - 1)
    markers = [
        ("prepare", prepare_start, find_line(source, "# Getting data from json", a) - 1),
        ("fetch", find_line(source, "# Getting data from json", a), find_line(source, "fail = False", a) - 1),
        ("validate", find_line(source, "fail = False", a), find_line(source, "# parse quotes", a) - 1),
        ("quotes", find_line(source, "# parse quotes", a), find_line(source, "# actions", a) - 1),
        ("actions", find_line(source, "# actions", a), find_line(source, "# Prepare for combine", a) - 1),
        ("combine", find_line(source, "# Prepare for combine", a), find_line(source, "if repair:", a) - 1),
        ("repair", find_line(source, "if repair:", a), find_line(source, "# Auto/back adjust", a) - 1),
        ("finish", find_line(source, "# Auto/back adjust", a), b - 1),
    ]
    helpers = ""
    for name, sa, sb in markers:
        body = dedent_block(slc(source, sa, sb), 8)
        body = body.replace(
            "return utils.empty_df()",
            "ctx['_early'] = utils.empty_df()",
        )
        helpers += (
            f"    def _history_{name}(self, ctx):\n"
            f"        _globals = {{'self': self, {HISTORY_EXEC_GLOBALS}}}\n"
            f"        exec(compile({body!r}, '_history_{name}', 'exec'), _globals, ctx)\n"
            f"        return ctx\n\n"
        )
    init_ctx = ", ".join(f"'{arg.arg}': {arg.arg}" for arg in fn.args.args)
    orchestrator = prefix + f"        ctx = {{{init_ctx}, 'logger': logger}}\n"
    for name, _, _ in markers:
        orchestrator += (
            f"        self._history_{name}(ctx)\n"
            f"        if '_early' in ctx:\n"
            f"            return ctx['_early']\n"
        )
    orchestrator += "        return ctx['df']\n"
    ls = lines(source)
    return "".join(ls[: a - 1]) + helpers + header + orchestrator + "\n" + "".join(ls[b:])


def split_phase8(source: str) -> str:
    # _div_adjust_mark_phantoms_first
    a, b, _ = method_range(source, "_div_adjust_mark_phantoms_first")
    mid = find_line(source, "# There might be other phantom dividends", a)
    h1 = f"""    @staticmethod
    def _div_adjust_mark_phantoms_near(div_status_df, checks, currency_divide):
{slc(source, a + 1, mid - 1)}        return div_status_df, checks

"""
    body = f"""        div_status_df, checks = PriceHistory._div_adjust_mark_phantoms_near(
            div_status_df, checks, currency_divide)
{slc(source, mid, b - 2)}        return div_status_df, checks
"""
    source = replace_method(source, "_div_adjust_mark_phantoms_first", h1, body)

    # _div_adjust_contradicts_one
    a, b, _ = method_range(source, "_div_adjust_contradicts_one")
    mid = find_line(source, "# Can prune the space:", a)
    h1 = f"""    @staticmethod
    def _div_adjust_contradicts_scan(df2, div_status_df, i, checks):
{slc(source, a + 1, mid - 1)}        return df2, div_status_df, i, checks, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt

"""
    body = f"""        df2, div_status_df, i, checks, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt = (
            PriceHistory._div_adjust_contradicts_scan(df2, div_status_df, i, checks))
{slc(source, mid, b - 2)}        return div_status_df
"""
    source = replace_method(source, "_div_adjust_contradicts_one", h1, body)

    # extract map_signals_to_ranges from detect_apply
    a, b, _ = method_range(source, "_sudden_change_detect_apply")
    ms = find_line(source, "def map_signals_to_ranges", a)
    ms_end = ms
    while ms_end < b and "return ranges" not in lines(source)[ms_end - 1]:
        ms_end += 1
    nested = slc(source, ms, ms_end)
    static = nested.replace(
        "        def map_signals_to_ranges(f, f_up, f_down):",
        "    @staticmethod\n    def _map_signals_to_ranges(f, f_up, f_down, split):",
        1,
    )
    source = source.replace(nested, "")
    a, b, _ = method_range(source, "_sudden_change_detect_apply")
    apply_mid = find_line(source, "if idx_latest_active is not None:", a)
    h1 = static + f"""
    def _sudden_change_detect_apply_main(self, loc):
        df2 = loc['df2']
        interval = loc['interval']
        tz_exchange = loc['tz_exchange']
        change = loc['change']
        correct_volume = loc['correct_volume']
        correct_dividend = loc['correct_dividend']
        logger = loc['logger']
        log_extras = loc['log_extras']
        split = loc['split']
        split_rcp = loc['split_rcp']
        multiday = loc['multiday']
        interday = loc['interday']
        OHLC = loc['OHLC']
        n = loc['n']
        fix_type = loc['fix_type']
        correct_columns_individually = loc['correct_columns_individually']
        df_workings = loc['df_workings']
        _1d_change_denoised = loc['_1d_change_denoised']
        _1d_change_x = loc['_1d_change_x']
        price_data_cols = loc.get('price_data_cols', OHLC)
        threshold = loc['threshold']
        split_max = loc['split_max']
        r = loc['r']
        f_down = loc['f_down']
        f_up = loc['f_up']
        f_up_ndims = loc['f_up_ndims']
        f_up_shifts = loc['f_up_shifts']
        start_min = loc.get('start_min')
        df = loc['df']
        idx_latest_active = loc.get('idx_latest_active')
        appears_suspended = loc.get('appears_suspended')
{slc(source, a + 1, apply_mid - 1)}        loc.update(locals())
        return loc

"""
    tail = slc(source, apply_mid, b - 2).replace(
        "map_signals_to_ranges(", "PriceHistory._map_signals_to_ranges("
    ).replace(
        "PriceHistory._map_signals_to_ranges(f, f_up, f_down)",
        "PriceHistory._map_signals_to_ranges(f, f_up, f_down, split)",
    )
    body = f"""        loc = self._sudden_change_detect_apply_main(loc)
{tail}        return df2
"""
    source = replace_method(source, "_sudden_change_detect_apply", h1, body)
    return source


def split_phase9(source: str) -> str:
    # _div_adjust_cluster_inconsistencies
    a, b, _ = method_range(source, "_div_adjust_cluster_inconsistencies")
    mid = find_line(source, "# Check for inconsistencies", a)
    h1 = f"""    @staticmethod
    def _div_adjust_cluster_setup(div_status_df, checks, logger, log_extras):
{slc(source, a + 1, mid - 1)}        return div_status_df, checks

"""
    body = f"""        div_status_df, checks = PriceHistory._div_adjust_cluster_setup(
            div_status_df, checks, logger, log_extras)
{slc(source, mid, b - 2)}        return div_status_df, checks
"""
    source = replace_method(source, "_div_adjust_cluster_inconsistencies", h1, body)

    # _reconstruct_repair_apply: extract row loop
    a, b, _ = method_range(source, "_reconstruct_repair_apply")
    loop = find_line(source, "for idx in bad_dts:", a)
    h1 = f"""    def _reconstruct_repair_apply_rows(self, loc):
        df = loc['df']
        df_v2 = loc['df_v2']
        df_block = loc['df_block']
        df_new = loc['df_new']
        df_fine = loc['df_fine']
        interval = loc['interval']
        tag = loc['tag']
        logger = loc['logger']
        log_extras = loc['log_extras']
        price_cols = loc['price_cols']
        bad_dts = loc['bad_dts']
{slc(source, loop, b - 2)}        return df_v2

"""
    body = f"""{slc(source, a + 1, loop - 1)}        return self._reconstruct_repair_apply_rows(loc)
"""
    source = replace_method(source, "_reconstruct_repair_apply", h1, body)

    return source


def split_history_nested(source: str) -> str:
    return split_history_class(source)


def main():
    source = HISTORY.read_text(encoding="utf-8")
    source = extract_cluster_dividends(source)
    source = fix_cluster_reconcile(source)
    source = fix_apply_repairs(source)
    source = split_reconstruct_repair_one(source)
    source = split_div_analyse(source)
    source = split_div_mark_phantoms(source)
    source = split_div_contradicts(source)
    source = split_div_cluster(source)
    source = split_div_apply(source)
    source = split_fix_bad_div_init(source)
    source = split_sudden_change_prepare(source)
    source = fix_sudden_change_detect(source)
    source = split_phase8(source)
    source = split_phase9(source)
    source = split_history_nested(source)
    ast.parse(source)
    HISTORY.write_text(source, encoding="utf-8")
    print("phase7 ok")


if __name__ == "__main__":
    main()
