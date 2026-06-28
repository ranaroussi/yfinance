"""Refactor remaining utils.py pylint smells."""
from pathlib import Path
import re

p = Path(__file__).resolve().parent.parent / "yfinance/utils.py"
text = p.read_text(encoding="utf-8")

# --- safe_merge_dfs helpers ---
if "_safe_merge_reindex_events" not in text:
    safe_helpers = '''

def _safe_merge_reindex_events(df, new_index, data_col_name):
    if len(new_index) == len(set(new_index)):
        df.index = new_index
        return df
    df["_NewIndex"] = new_index
    if data_col_name in ["Dividends", "Capital Gains"]:
        df = df.groupby("_NewIndex").sum()
        df.index.name = None
    elif data_col_name == "Stock Splits":
        df = df.groupby("_NewIndex").prod()
        df.index.name = None
    else:
        raise YFException(f"New index contains duplicates but unsure how to aggregate for '{data_col_name}'")
    if "_NewIndex" in df.columns:
        df = df.drop("_NewIndex", axis=1)
    return df


def _safe_merge_calc_indices(df_main, df_sub, intraday, td):
    if intraday:
        df_main = df_main.copy()
        df_sub = df_sub.copy()
        df_main['_date'] = df_main.index.date
        df_sub['_date'] = df_sub.index.date
        indices = _np.searchsorted(
            _np.append(df_main['_date'], [df_main['_date'].iloc[-1] + td]),
            df_sub['_date'], side='left')
        df_main = df_main.drop('_date', axis=1)
        df_sub = df_sub.drop('_date', axis=1)
    else:
        indices = _np.searchsorted(
            _np.append(df_main.index, df_main.index[-1] + td), df_sub.index, side='right')
        indices -= 1
    return df_main, df_sub, indices


def _safe_merge_mark_out_of_range(indices, df_main, df_sub, intraday, td):
    indices = indices.copy()
    if intraday:
        for i in range(len(df_sub.index)):
            dt = df_sub.index[i].date()
            if dt < df_main.index[0].date() or dt >= df_main.index[-1].date() + _datetime.timedelta(days=1):
                indices[i] = -1
    else:
        for i in range(len(df_sub.index)):
            dt = df_sub.index[i]
            if dt < df_main.index[0] or dt >= df_main.index[-1] + td:
                indices[i] = -1
    return indices


def _safe_merge_add_empty_rows(df_main, df_sub, indices, interval, td, data_col):
    empty_row_data = {**{c: [_np.nan] for c in const._PRICE_COLNAMES_}, 'Volume': [0]}
    f_out = indices == -1
    if interval == '1d':
        for i in _np.where(f_out)[0]:
            dt = df_sub.index[i]
            get_yf_logger().debug(f"Adding out-of-range {data_col} @ {dt.date()} in new prices row of NaNs")
            df_main = _pd.concat([df_main, _pd.DataFrame(data=empty_row_data, index=[dt])], sort=True)
    else:
        last_dt = df_main.index[-1]
        next_start = last_dt + td
        next_end = next_start + td
        for i in _np.where(f_out)[0]:
            dt = df_sub.index[i]
            if next_start <= dt < next_end:
                get_yf_logger().debug(f"Adding out-of-range {data_col} @ {dt.date()} in new prices row of NaNs")
                df_main = _pd.concat([df_main, _pd.DataFrame(data=empty_row_data, index=[dt])], sort=True)
    return df_main.sort_index()


def _safe_merge_handle_out_of_range(df_main, df_sub, indices, intraday, interval, td, data_col):
    f_out = indices == -1
    if not f_out.any():
        return df_main, df_sub, indices
    if intraday:
        df_sub = df_sub[~f_out]
        if df_sub.empty:
            df_main['Dividends'] = 0.0
            return df_main, df_sub, None
        df_main, df_sub, indices = _safe_merge_calc_indices(df_main, df_sub, intraday, td)
        indices = _safe_merge_mark_out_of_range(indices, df_main, df_sub, intraday, td)
        return df_main, df_sub, indices
    df_main = _safe_merge_add_empty_rows(df_main, df_sub, indices, interval, td, data_col)
    df_main, df_sub, indices = _safe_merge_calc_indices(df_main, df_sub, intraday, td)
    indices = _safe_merge_mark_out_of_range(indices, df_main, df_sub, intraday, td)
    return df_main, df_sub, indices


def _safe_merge_finalize_out_of_range(df_sub, indices, intraday, interval, data_col):
    f_out = indices == -1
    if not f_out.any():
        return df_sub, indices
    if intraday or interval in ['1d', '1wk']:
        raise YFException(
            f"The following '{data_col}' events are out-of-range, did not expect with interval {interval}: "
            f"{df_sub.index[f_out]}")
    get_yf_logger().debug(f'Discarding these {data_col} events:\\n' + str(df_sub[f_out]))
    df_sub = df_sub[~f_out].copy()
    return df_sub, indices[~f_out]

'''
    text = text.replace("def safe_merge_dfs(df_main, df_sub, interval):", safe_helpers + "def safe_merge_dfs(df_main, df_sub, interval):")

new_safe = '''def safe_merge_dfs(df_main, df_sub, interval):
    if df_main.empty:
        return df_main

    data_col = [c for c in df_sub.columns if c not in df_main][0]
    df_main = df_main.sort_index()
    intraday = interval.endswith('m') or interval.endswith('s')
    td = _interval_to_timedelta(interval)

    df_main, df_sub, indices = _safe_merge_calc_indices(df_main, df_sub, intraday, td)
    indices = _safe_merge_mark_out_of_range(indices, df_main, df_sub, intraday, td)
    df_main, df_sub, indices = _safe_merge_handle_out_of_range(
        df_main, df_sub, indices, intraday, interval, td, data_col)
    if indices is None:
        return df_main

    df_sub, indices = _safe_merge_finalize_out_of_range(df_sub, indices, intraday, interval, data_col)
    new_index = df_main.index[indices]
    df_sub = _safe_merge_reindex_events(df_sub, new_index, data_col)
    df = df_main.join(df_sub)
    if sum(~df[data_col].isna()) < df_sub.shape[0]:
        raise YFException('Data was lost in merge, investigate')
    return df'''

text = re.sub(r'def safe_merge_dfs\(df_main, df_sub, interval\):.*?return df\n\n\ndef fix_Yahoo_dst_issue',
              new_safe + '\n\n\ndef fix_Yahoo_dst_issue', text, count=1, flags=re.DOTALL)

# --- format_history_metadata ---
if "_format_md_scalar_timestamps" not in text:
    fmt_helpers = '''

def _format_md_scalar_timestamps(md, tz):
    for k in ["firstTradeDate", "regularMarketTime"]:
        if k in md and md[k] is not None and isinstance(md[k], int):
            md[k] = _pd.to_datetime(md[k], unit='s', utc=True).tz_convert(tz)
    if "currentTradingPeriod" not in md:
        return
    for m in ["regular", "pre", "post"]:
        period = md["currentTradingPeriod"].get(m)
        if period is None or not isinstance(period.get("start"), int):
            continue
        for t in ["start", "end"]:
            period[t] = _pd.to_datetime(period[t], unit='s', utc=True).tz_convert(tz)
        period.pop("gmtoffset", None)
        period.pop("timezone", None)


def _format_trading_periods_list(tps, tz):
    df = _pd.DataFrame.from_records(_np.hstack(tps))
    df = df.drop(["timezone", "gmtoffset"], axis=1)
    df["start"] = _pd.to_datetime(df["start"], unit='s', utc=True).dt.tz_convert(tz)
    df["end"] = _pd.to_datetime(df["end"], unit='s', utc=True).dt.tz_convert(tz)
    return df


def _format_trading_periods_dict(tps, tz):
    pre_df = _pd.DataFrame.from_records(_np.hstack(tps["pre"]))
    post_df = _pd.DataFrame.from_records(_np.hstack(tps["post"]))
    regular_df = _pd.DataFrame.from_records(_np.hstack(tps["regular"]))
    pre_df = pre_df.rename(columns={"start": "pre_start", "end": "pre_end"}).drop(["timezone", "gmtoffset"], axis=1)
    post_df = post_df.rename(columns={"start": "post_start", "end": "post_end"}).drop(["timezone", "gmtoffset"], axis=1)
    regular_df = regular_df.drop(["timezone", "gmtoffset"], axis=1)
    cols = ["pre_start", "pre_end", "start", "end", "post_start", "post_end"]
    df = regular_df.join(pre_df).join(post_df)
    for c in cols:
        df[c] = _pd.to_datetime(df[c], unit='s', utc=True).dt.tz_convert(tz)
    return df[cols]


def _format_trading_periods_index(df, tz):
    df.index = _pd.to_datetime(df["start"].dt.date)
    df.index = df.index.tz_localize(tz)
    df.index.name = "Date"
    return df

'''
    text = text.replace("def format_history_metadata(md, tradingPeriodsOnly=True):", fmt_helpers + "def format_history_metadata(md, tradingPeriodsOnly=True):")

new_fmt = '''def format_history_metadata(md, tradingPeriodsOnly=True):
    if not isinstance(md, dict) or len(md) == 0:
        return md
    tz = md["exchangeTimezoneName"]
    if not tradingPeriodsOnly:
        _format_md_scalar_timestamps(md, tz)
    tps = md.get("tradingPeriods")
    if tps is None or tps == {"pre": [], "post": []}:
        return md
    if isinstance(tps, list):
        df = _format_trading_periods_list(tps, tz)
    elif isinstance(tps, dict):
        df = _format_trading_periods_dict(tps, tz)
    else:
        return md
    md["tradingPeriods"] = _format_trading_periods_index(df, tz)
    return md'''

text = re.sub(r'def format_history_metadata\(md, tradingPeriodsOnly=True\):.*?return md\n\n\nclass ProgressBar',
              new_fmt + '\n\n\nclass ProgressBar', text, count=1, flags=re.DOTALL)

# --- generate_list_table ---
if "_format_table_scalar_values" not in text:
    gen_helpers = '''

def _format_table_scalar_values(table, values, bullets):
    lengths = [len(str(v)) for v in values]
    if bullets and max(lengths) > 5:
        table += ' '*5 + "-\\n"
        for value in sorted(values):
            table += ' '*7 + f"- {value}\\n"
    else:
        table += ' '*5 + f"- {', '.join(sorted(values))}\\n"
    return table


def _format_table_k2_values(k2, k2_values):
    if isinstance(k2_values, set):
        k2_values = list(k2_values)
    elif isinstance(k2_values, dict) and len(k2_values) == 0:
        k2_values = []
    if isinstance(k2_values, list):
        k2_values = sorted(k2_values)
        if all(isinstance(v, (int, float, str)) for v in k2_values):
            return _re.sub(r"[{}\\[\\]']", "", str(k2_values))
    return str(k2_values)


def _append_table_block_line(table_add, k2, k2_values_str, block_format):
    if '\\n' in k2_values_str:
        table_add += '| ' + f"{k2}: " + "\\n"
        for j, line in enumerate(k2_values_str.split('\\n')):
            table_add += ' '*7 + '|' + ' '*5 + line
            if j < len(k2_values_str.split('\\n')) - 1:
                table_add += "\\n"
    else:
        prefix = '| ' if block_format else '* '
        table_add += prefix + f"{k2}: " + k2_values_str + "\\n"
    return table_add

'''
    text = text.replace("def generate_list_table_from_dict_universal(data: dict", gen_helpers + "def generate_list_table_from_dict_universal(data: dict")

new_gen = '''def generate_list_table_from_dict_universal(data: dict, bullets: bool=True, title: str=None, concat_keys=[]) -> str:
    """
    Generate a list-table for the docstring showing permitted keys/values.
    """
    table = _generate_table_configurations(title)
    for k, values in data.items():
        table += ' '*3 + f"* - {k}\\n"
        if not isinstance(values, dict):
            table = _format_table_scalar_values(table, values, bullets)
            continue
        table_add = ''
        concat_short_lines = k in concat_keys
        k_keys = sorted(list(values.keys()))
        block_format = 'query' in k_keys
        current_line = ''
        for i, k2 in enumerate(k_keys):
            k2_values_str = _format_table_k2_values(k2, values[k2])
            if len(current_line) > 0 and (len(current_line) + len(k2_values_str) > 40):
                table_add += current_line + '\\n'
                current_line = ''
            if concat_short_lines:
                if current_line == '':
                    current_line += ' '*5 + ("- " if i == 0 else "  ") + '| '
                else:
                    current_line += '.  '
                current_line += f"{k2}: " + k2_values_str
            else:
                table_add += ' '*5 + ("- " if i == 0 else "  ")
                table_add = _append_table_block_line(table_add, k2, k2_values_str, block_format)
        if current_line:
            table_add += current_line + '\\n'
        table += table_add
    return table'''

text = re.sub(
    r'def generate_list_table_from_dict_universal\(data: dict.*?return table\n',
    new_gen + '\n',
    text,
    count=1,
    flags=re.DOTALL,
)

p.write_text(text, encoding="utf-8")
print("utils remaining refactored")
