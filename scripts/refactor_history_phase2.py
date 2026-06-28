"""Phase 2: extract helpers from remaining large PriceHistory methods."""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"


def get_method_range(source: str, name: str) -> tuple[int, int, ast.FunctionDef]:
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return fn.lineno, fn.end_lineno, fn


def get_body_lines(source: str, start: int, end: int) -> list[str]:
    lines = source.splitlines(keepends=True)
    return lines[start:end]


def set_body_indent(lines: list[str], base_indent: str = "        ") -> str:
    return "".join(lines).replace("        ", base_indent, 1) if lines else ""


def replace_method(source: str, name: str, new_helpers: str, new_body: str) -> str:
    start, end, fn = get_method_range(source, name)
    lines = source.splitlines(keepends=True)
    header_lines = lines[start - 1:end]
    header = header_lines[0]
    for hl in header_lines[1:]:
        if hl.strip().startswith(('"""', "'''", "@")) or (hl.strip() and not hl.startswith("        ")):
            header += hl
        else:
            break
    replacement = new_helpers + header + new_body + "\n"
    return "".join(lines[: start - 1]) + replacement + "".join(lines[end:])


def extract_inner(source: str, method: str, helper_name: str, helper_params: str,
                  rel_start: int, rel_end: int, decorators: str = "") -> tuple[str, str]:
    """Move relative line range from method body into a new helper; return helper source and removed range marker."""
    mstart, mend, _ = get_method_range(source, method)
    body = get_body_lines(source, mstart, mend)
    # body[0] is def line; content starts at index where 8-space indent begins
    chunk = body[rel_start:rel_end]
    helper = f"{decorators}    def {helper_name}({helper_params}):\n{''.join(chunk)}\n"
    return helper, (rel_start, rel_end)


def main():
    source = HISTORY.read_text(encoding="utf-8")

    # --- _repair_capital_gains ---
    cg_helpers = textwrap.dedent('''
    def _capital_gains_mean_price_change(self, df):
        df = df.copy().sort_index()
        df['Price_Change%'] = df['Close'].pct_change(fill_method=None).abs()
        no_distributions = (df['Dividends'] == 0) & (df['Capital Gains'] == 0)
        price_drop_pct_mean = df.loc[no_distributions, 'Price_Change%'].mean()
        df = df.drop('Price_Change%', axis=1)
        return df, price_drop_pct_mean

    def _capital_gains_detect_double_counts(self, df, price_drop_pct_mean, debug=False):
        dts = df[df['Capital Gains'] > 0].index
        c = df['Close'].to_numpy()
        dcs = {}
        for dt in dts:
            idx = df.index.get_loc(dt)
            if idx <= 0:
                continue
            dividend = df['Dividends'].iloc[idx]
            capital_gains = df['Capital Gains'].iloc[idx]
            if dividend < capital_gains:
                continue
            div_pct = dividend / c[idx - 1]
            cg_pct = capital_gains / c[idx - 1]
            price_drop_pct = (c[idx - 1] - c[idx]) / c[idx - 1]
            price_drop_pct_excl_vol = price_drop_pct - price_drop_pct_mean
            diff_div = abs(price_drop_pct_excl_vol - div_pct)
            diff_total = abs(price_drop_pct_excl_vol - (div_pct + cg_pct))
            dcs[idx] = diff_div < diff_total
            if debug:
                print(f"# {dt.date()}: div = {div_pct*100:.1f}%, cg = {cg_pct*100:.1f}%")
        return dcs

    def _capital_gains_apply_corrections(self, df, dcs, logger, log_extras, debug=False):
        ac = df['Adj Close'].to_numpy()
        c = df['Close'].to_numpy()
        for idx in dcs.keys():
            dt = df.index[idx]
            dividend = df['Dividends'].iloc[idx]
            capital_gains = df['Capital Gains'].iloc[idx]
            dividend_true = dividend - capital_gains
            df.loc[dt, 'Dividends'] = dividend_true
            adj_before = (ac[idx - 1] / c[idx - 1]) / (ac[idx] / c[idx])
            adj_correct = 1.0 - (dividend_true + capital_gains) / c[idx - 1]
            correction = adj_correct / adj_before
            df.loc[:dt - _datetime.timedelta(1), 'Adj'] *= correction
            df.loc[:dt, 'Repaired?'] = True
            logger.info(
                f"Repaired capital-gains double-count at {dt.date()}. Adj correction = {correction:.4f}",
                extra=log_extras,
            )
            if debug:
                df.loc[dt, 'correction'] = correction
        return df
    ''').strip("\n")
    cg_helpers = textwrap.indent(cg_helpers, "    ") + "\n\n"

    cg_body = textwrap.dedent('''
        if 'Capital Gains' not in df.columns or (df['Capital Gains'] == 0).all():
            return df
        debug = False
        logger = utils.get_yf_logger()
        log_extras = {'yf_cat': 'repair-capital-gains', 'yf_symbol': self.ticker}
        df, price_drop_pct_mean = self._capital_gains_mean_price_change(df)
        if 'Repaired?' not in df.columns:
            df['Repaired?'] = False
        df['Adj'] = df['Adj Close'] / df['Close']
        dcs = self._capital_gains_detect_double_counts(df, price_drop_pct_mean, debug)
        if dcs and sum(dcs.values()) / len(dcs) >= 0.666:
            df = self._capital_gains_apply_corrections(df, dcs, logger, log_extras, debug)
        df['Adj Close'] = df['Close'] * df['Adj']
        if not debug:
            df = df.drop('Adj', axis=1)
        return df
    ''')

    source = replace_method(source, "_repair_capital_gains", cg_helpers, textwrap.indent(cg_body, "        "))

    # --- _fix_zeroes ---
    z_helpers = textwrap.dedent('''
    def _zeroes_localize_df(self, df, tz_exchange):
        df2 = df.copy()
        if df2.index.tz is None:
            df2.index = df2.index.tz_localize(tz_exchange)
        elif df2.index.tz != tz_exchange:
            df2.index = df2.index.tz_convert(tz_exchange)
        return df2

    def _zeroes_filter_intraday_bad_days(self, df, df2, price_cols, intraday):
        f_prices_bad = (df2[price_cols] == 0.0) | df2[price_cols].isna()
        df2_reserve = None
        if not intraday:
            return df2, df, f_prices_bad, df2_reserve
        grp = pd.Series(f_prices_bad.any(axis=1), name="nan").groupby(f_prices_bad.index.date)
        nan_pct = grp.sum() / grp.count()
        dts = nan_pct.index[nan_pct > 0.5]
        f_zero_or_nan_ignore = np.isin(f_prices_bad.index.date, dts)
        df2_reserve = df2[f_zero_or_nan_ignore]
        df2 = df2[~f_zero_or_nan_ignore]
        if df2.empty:
            return None, df, f_prices_bad, df2_reserve
        df2 = df2.copy()
        f_prices_bad = (df2[price_cols] == 0.0) | df2[price_cols].isna()
        return df2, df, f_prices_bad, df2_reserve

    def _zeroes_bad_volume_mask(self, df2, intraday):
        f_change = df2["High"].to_numpy() != df2["Low"].to_numpy()
        if self.ticker.endswith("=X"):
            return f_change, None
        f_high_low_good = (~df2["High"].isna().to_numpy()) & (~df2["Low"].isna().to_numpy())
        f_vol_zero = (df2["Volume"] == 0).to_numpy()
        f_vol_bad = f_vol_zero & f_high_low_good & f_change
        if not intraday:
            close_diff = df2['Close'].diff()
            close_diff.iloc[0] = 0
            close_chg_pct_abs = np.abs(close_diff / df2['Close'])
            f_bad_price_chg = (close_chg_pct_abs > 0.05).to_numpy() & f_vol_zero
            f_vol_bad = f_vol_bad | f_bad_price_chg
        return f_change, f_vol_bad

    def _zeroes_tag_and_reconstruct(self, df, df2, price_cols, f_prices_bad, f_vol_bad, f_change, interval, prepost, logger, log_extras):
        data_cols = price_cols + ["Volume"]
        tag = -1.0
        f_prices_bad = f_prices_bad.to_numpy()
        for i, c in enumerate(price_cols):
            df2.loc[f_prices_bad[:, i], c] = tag
        if f_vol_bad is not None:
            df2.loc[f_vol_bad, "Volume"] = tag
        f_vol_zero_or_nan = (df2["Volume"].to_numpy() == 0) | (df2["Volume"].isna().to_numpy())
        df2.loc[f_prices_bad.any(axis=1) & f_vol_zero_or_nan, "Volume"] = tag
        df2.loc[f_change & f_vol_zero_or_nan, "Volume"] = tag
        df2_tagged = df2[data_cols].to_numpy() == tag
        n_before = df2_tagged.sum()
        dts_tagged = df2.index[df2_tagged.any(axis=1)]
        df2 = self._reconstruct_intervals_batch(df2, interval, prepost, tag)
        df2_tagged = df2[data_cols].to_numpy() == tag
        n_after = df2_tagged.sum()
        n_fixed = n_before - n_after
        if n_fixed > 0:
            msg = f"{self.ticker}: fixed {n_fixed}/{n_before} value=0 errors in {interval} price data"
            if n_fixed < 4:
                dts_not_repaired = df2.index[df2_tagged.any(axis=1)]
                dts_repaired = sorted(list(set(dts_tagged).difference(dts_not_repaired)))
                msg += f": {dts_repaired}"
            logger.debug(msg, extra=log_extras)
        return df2, tag, data_cols
    ''').strip("\n")
    z_helpers = textwrap.indent(z_helpers, "    ") + "\n\n"

    z_body = textwrap.dedent('''
        if df.empty:
            return df
        logger = utils.get_yf_logger()
        log_extras = {'yf_cat': 'price-repair-zeroes', 'yf_interval': interval, 'yf_symbol': self.ticker}
        intraday = interval[-1] in ("m", 'h')
        df = df.sort_index()
        df2 = self._zeroes_localize_df(df, tz_exchange)
        price_cols = [c for c in _PRICE_COLNAMES_ if c in df2.columns]
        df2, df, f_prices_bad, df2_reserve = self._zeroes_filter_intraday_bad_days(df, df2, price_cols, intraday)
        if df2 is None:
            if 'Repaired?' not in df.columns:
                df['Repaired?'] = False
            return df
        f_change, f_vol_bad = self._zeroes_bad_volume_mask(df2, intraday)
        if 'Stock Splits' in df2.columns:
            f_split = (df2['Stock Splits'] != 0.0).to_numpy()
            if f_split.any():
                f_change_expected_but_missing = f_split & ~f_change
                if f_change_expected_but_missing.any():
                    f_prices_bad[f_change_expected_but_missing] = True
        f_bad_rows = f_prices_bad.to_numpy().any(axis=1)
        if f_vol_bad is not None:
            f_bad_rows = f_bad_rows | f_vol_bad
        if not f_bad_rows.any():
            logger.debug("No price=0 errors to repair", extra=log_extras)
            if "Repaired?" not in df.columns:
                df["Repaired?"] = False
            return df
        if f_prices_bad.to_numpy().sum() == len(price_cols) * len(df2):
            logger.debug("No good data for calibration so cannot fix price=0 bad data", extra=log_extras)
            if "Repaired?" not in df.columns:
                df["Repaired?"] = False
            return df
        df2, tag, data_cols = self._zeroes_tag_and_reconstruct(
            df, df2, price_cols, f_prices_bad, f_vol_bad, f_change, interval, prepost, logger, log_extras)
        if df2_reserve is not None:
            if "Repaired?" not in df2_reserve.columns:
                df2_reserve["Repaired?"] = False
            df2 = pd.concat([df2, df2_reserve]).sort_index()
        f = df2[data_cols].to_numpy() == tag
        for j, c in enumerate(data_cols):
            fj = f[:, j]
            if fj.any():
                df2.loc[fj, c] = df.loc[fj, c]
        return df2
    ''')

    source = replace_method(source, "_fix_zeroes", z_helpers, textwrap.indent(z_body, "        "))

    HISTORY.write_text(source, encoding="utf-8")
    print("Phase 2 done: _repair_capital_gains, _fix_zeroes")


if __name__ == "__main__":
    main()
