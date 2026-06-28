"""Phase 3: extract helpers from unit mixups, zeroes, reconstruct, history."""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"


def get_method_range(source: str, name: str) -> tuple[int, int]:
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PriceHistory")
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return fn.lineno, fn.end_lineno


def slice_abs(source: str, start: int, end: int) -> str:
    return "".join(source.splitlines(keepends=True)[start - 1:end])


def replace_method(source: str, name: str, helpers: str, body: str) -> str:
    start, end = get_method_range(source, name)
    lines = source.splitlines(keepends=True)
    header = lines[start - 1]
    i = start
    while i < end:
        line = lines[i]
        if line.strip().startswith(('"""', "'''", "@")) or (line.strip() and not line.startswith("        ")):
            header += line
            i += 1
        else:
            break
    return "".join(lines[: start - 1]) + helpers + header + body + "\n" + "".join(lines[end:])


def main():
    source = HISTORY.read_text(encoding="utf-8")

    # --- _fix_unit_random_mixups ---
    ur_helpers = slice_abs(source, 1137, 1289)
    # We'll rewrite with helpers instead of copying wholesale
    ur_helpers = textwrap.dedent('''
    def _unit_random_ensure_repaired_col(self, df):
        if "Repaired?" not in df.columns:
            df["Repaired?"] = False
        return df

    def _unit_random_prepare(self, df, tz_exchange, logger, log_extras):
        if df.empty or df.shape[0] <= 1:
            if df.shape[0] == 1:
                logger.debug("Cannot check single-row table for 100x price errors", extra=log_extras)
            return None
        df2 = df.copy()
        if df2.index.tz is None:
            df2.index = df2.index.tz_localize(tz_exchange)
        elif df2.index.tz != tz_exchange:
            df2.index = df2.index.tz_convert(tz_exchange)
        from scipy import ndimage as _ndimage
        data_cols = [c for c in ["High", "Open", "Low", "Close", "Adj Close"] if c in df2.columns]
        f_zeroes = (df2[data_cols] == 0).any(axis=1).to_numpy()
        if f_zeroes.any():
            df2_zeroes, df2, df_orig = df2[f_zeroes], df2[~f_zeroes], df[~f_zeroes]
        else:
            df2_zeroes, df_orig = None, df
        if df2.shape[0] <= 1:
            logger.info("Insufficient good data for detecting 100x price errors", extra=log_extras)
            return None
        return df2, df_orig, df2_zeroes, data_cols, _ndimage

    def _unit_random_detect_outliers(self, df2, data_cols, _ndimage):
        df2_data = df2[data_cols].to_numpy()
        median = _ndimage.median_filter(df2_data, size=(3, 3), mode="wrap")
        ratio = df2_data / median
        ratio_rounded = (ratio / 20).round() * 20
        ratio_rcp = 1.0 / ratio
        ratio_rcp_rounded = (ratio_rcp / 20).round() * 20
        f = ratio_rounded == 100
        f_rcp = (ratio_rounded == 100) | (ratio_rcp_rounded == 100)
        return f | f_rcp, f, f_rcp, df2_data

    def _unit_random_crude_fix_remaining(self, df2, df, data_cols, f, f_rcp, tag):
        n_after = (df2[data_cols].to_numpy() == tag).sum()
        if n_after <= 0:
            return n_after, n_after
        f = (df2[data_cols].to_numpy() == tag) & f
        for i in range(f.shape[0]):
            fi = f[i, :]
            if not fi.any():
                continue
            idx = df2.index[i]
            for c in ['Open', 'Close']:
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df.loc[idx, c] * 0.01
            for c, op in [("High", max), ("Low", min)]:
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = op(df2.loc[idx, ["Open", "Close"]].max(), df2.loc[idx, ["Open", "Close"]].min()) if c == "High" else min(df2.loc[idx, ["Open", "Close"]])
        f_rcp = (df2[data_cols].to_numpy() == tag) & f_rcp
        for i in range(f_rcp.shape[0]):
            fi = f_rcp[i, :]
            if not fi.any():
                continue
            idx = df2.index[i]
            for c in ['Open', 'Close']:
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df.loc[idx, c] * 100.0
            for c in ['High', 'Low']:
                j = data_cols.index(c)
                if fi[j]:
                    vals = df2.loc[idx, ["Open", "Close"]]
                    df2.loc[idx, c] = vals.max() if c == 'High' else vals.min()
        return n_after, (df2[data_cols].to_numpy() == tag).sum()
    ''').strip("\n")
    # Fix crude fix - the High/Low logic was wrong in my shortcut. Use original code block instead.
    ur_helpers = slice_abs(source, 1137, 1202)  # read original for reference - skip

    ur_helpers = textwrap.dedent('''
    def _unit_random_prepare(self, df, tz_exchange, logger, log_extras):
        df2 = df.copy()
        if df2.index.tz is None:
            df2.index = df2.index.tz_localize(tz_exchange)
        elif df2.index.tz != tz_exchange:
            df2.index = df2.index.tz_convert(tz_exchange)
        from scipy import ndimage as _ndimage
        data_cols = [c for c in ["High", "Open", "Low", "Close", "Adj Close"] if c in df2.columns]
        f_zeroes = (df2[data_cols] == 0).any(axis=1).to_numpy()
        if f_zeroes.any():
            df2_zeroes = df2[f_zeroes]
            df2 = df2[~f_zeroes]
            df_orig = df[~f_zeroes]
            return df2_zeroes, df2, df_orig, data_cols, _ndimage
        return None, df2, df, data_cols, _ndimage

    def _unit_random_outlier_flags(self, df2, data_cols, _ndimage):
        df2_data = df2[data_cols].to_numpy()
        median = _ndimage.median_filter(df2_data, size=(3, 3), mode="wrap")
        ratio = df2_data / median
        ratio_rounded = (ratio / 20).round() * 20
        f = ratio_rounded == 100
        ratio_rcp = 1.0 / ratio
        ratio_rcp_rounded = (ratio_rcp / 20).round() * 20
        f_rcp = (ratio_rounded == 100) | (ratio_rcp_rounded == 100)
        return f | f_rcp, f, f_rcp, df2_data

    def _unit_random_apply_crude_fixes(self, df2, df, data_cols, f, f_rcp, tag):
        f_tag = (df2[data_cols].to_numpy() == tag) & f
        for i in range(f_tag.shape[0]):
            fi = f_tag[i, :]
            if not fi.any():
                continue
            idx = df2.index[i]
            for c in ['Open', 'Close']:
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df.loc[idx, c] * 0.01
            if fi[data_cols.index("High")]:
                df2.loc[idx, "High"] = df2.loc[idx, ["Open", "Close"]].max()
            if fi[data_cols.index("Low")]:
                df2.loc[idx, "Low"] = df2.loc[idx, ["Open", "Close"]].min()
        f_rcp_tag = (df2[data_cols].to_numpy() == tag) & f_rcp
        for i in range(f_rcp_tag.shape[0]):
            fi = f_rcp_tag[i, :]
            if not fi.any():
                continue
            idx = df2.index[i]
            for c in ['Open', 'Close']:
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df.loc[idx, c] * 100.0
            if fi[data_cols.index("High")]:
                df2.loc[idx, "High"] = df2.loc[idx, ["Open", "Close"]].max()
            if fi[data_cols.index("Low")]:
                df2.loc[idx, "Low"] = df2.loc[idx, ["Open", "Close"]].min()
        return (df2[data_cols].to_numpy() == tag).sum()

    def _unit_random_restore_unfixed(self, df2, df_orig, df2_zeroes, data_cols, tag):
        f_either = df2[data_cols].to_numpy() == tag
        for j, c in enumerate(data_cols):
            fj = f_either[:, j]
            if fj.any():
                df2.loc[fj, c] = df_orig.loc[fj, c]
        if df2_zeroes is not None:
            if "Repaired?" not in df2_zeroes.columns:
                df2_zeroes["Repaired?"] = False
            df2 = pd.concat([df2, df2_zeroes]).sort_index()
            df2.index = pd.to_datetime(df2.index)
        return df2
    ''').strip("\n")
    ur_helpers = textwrap.indent(ur_helpers, "    ") + "\n\n"

    ur_body = textwrap.dedent('''
        if df.empty:
            return df
        logger = utils.get_yf_logger()
        log_extras = {'yf_cat': 'price-repair-100x', 'yf_interval': interval, 'yf_symbol': self.ticker}
        if df.shape[0] <= 1:
            if df.shape[0] == 1:
                logger.debug("Cannot check single-row table for 100x price errors", extra=log_extras)
            if "Repaired?" not in df.columns:
                df["Repaired?"] = False
            return df
        df2_zeroes, df2, df_orig, data_cols, _ndimage = self._unit_random_prepare(df, tz_exchange, logger, log_extras)
        if df2_zeroes is not None and df2.shape[0] <= 1:
            logger.info("Insufficient good data for detecting 100x price errors", extra=log_extras)
            if "Repaired?" not in df.columns:
                df["Repaired?"] = False
            return df
        f_either, f, f_rcp, df2_data = self._unit_random_outlier_flags(df2, data_cols, _ndimage)
        if not f_either.any():
            logger.debug("No sporadic 100x errors", extra=log_extras)
            if "Repaired?" not in df.columns:
                df["Repaired?"] = False
            return df
        tag = -1.0
        for i, c in enumerate(data_cols):
            df2.loc[f_either[:, i], c] = tag
        n_before = (df2_data == tag).sum()
        df2 = self._reconstruct_intervals_batch(df2, interval, prepost, tag)
        n_after = (df2[data_cols].to_numpy() == tag).sum()
        n_after_crude = self._unit_random_apply_crude_fixes(df2, df, data_cols, f, f_rcp, tag) if n_after > 0 else n_after
        n_fixed = n_before - n_after_crude
        if n_fixed > 0:
            msg = f"fixed {n_fixed}/{n_before} currency unit mixups "
            if n_after - n_after_crude > 0:
                msg += f"({n_after - n_after_crude} crudely)"
            logger.info(msg, extra=log_extras)
        return self._unit_random_restore_unfixed(df2, df_orig, df2_zeroes, data_cols, tag)
    ''')

    source = replace_method(source, "_fix_unit_random_mixups", ur_helpers, textwrap.indent(ur_body, "        "))

    # --- _fix_zeroes: reduce branches ---
    z_extra = textwrap.indent(textwrap.dedent('''
    def _zeroes_early_exit(self, df, f_prices_bad, f_vol_bad, f_change, price_cols, df2, logger, log_extras):
        if 'Stock Splits' in df2.columns:
            f_split = (df2['Stock Splits'] != 0.0).to_numpy()
            if f_split.any():
                missing = f_split & ~f_change
                if missing.any():
                    f_prices_bad[missing] = True
        f_bad_rows = f_prices_bad.to_numpy().any(axis=1)
        if f_vol_bad is not None:
            f_bad_rows = f_bad_rows | f_vol_bad
        if not f_bad_rows.any():
            logger.debug("No price=0 errors to repair", extra=log_extras)
            return self._zeroes_ensure_repaired_col(df)
        if f_prices_bad.to_numpy().sum() == len(price_cols) * len(df2):
            logger.debug("No good data for calibration so cannot fix price=0 bad data", extra=log_extras)
            return self._zeroes_ensure_repaired_col(df)
        return None

    def _zeroes_ensure_repaired_col(self, df):
        if "Repaired?" not in df.columns:
            df["Repaired?"] = False
        return df
    ''').strip("\n"), "    ") + "\n\n"

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
            return self._zeroes_ensure_repaired_col(df)
        f_change, f_vol_bad = self._zeroes_bad_volume_mask(df2, intraday)
        early = self._zeroes_early_exit(df, f_prices_bad, f_vol_bad, f_change, price_cols, df2, logger, log_extras)
        if early is not None:
            return early
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

    start, end = get_method_range(source, "_fix_zeroes")
    lines = source.splitlines(keepends=True)
    # insert z_extra before _fix_zeroes
    source = "".join(lines[: start - 1]) + z_extra + "".join(lines[start - 1:])
    source = replace_method(source, "_fix_zeroes", "", textwrap.indent(z_body, "        "))

    HISTORY.write_text(source, encoding="utf-8")
    print("Phase 3 done: _fix_unit_random_mixups, _fix_zeroes")


if __name__ == "__main__":
    main()
