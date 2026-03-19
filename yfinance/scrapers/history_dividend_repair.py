"""Dividend-adjustment repair helpers extracted from history.py."""

import bisect
import datetime as _datetime
from math import isclose
from typing import cast

import numpy as np
import pandas as pd

from yfinance import utils


def fix_bad_div_adjust(price_history, df, interval, _currency):
    """Repair dividend amounts or dividend adjustment factors that Yahoo returned incorrectly."""
    return _DividendRepairRunner(price_history, df, interval, _currency).repair()


def _cluster_dividends(frame, column="div", threshold=7):
    n = len(frame)
    sorted_df = frame.sort_values(column)
    clusters = []
    current_dts = [sorted_df.index[0]]
    current_vals = [sorted_df[column].iloc[0]]
    for i in range(1, n):
        dt = sorted_df.index[i]
        div = sorted_df[column].iloc[i]
        if (div / np.mean(current_vals)) < threshold:
            current_dts.append(dt)
            current_vals.append(div)
        else:
            clusters.append(current_dts)
            current_dts = [dt]
            current_vals = [div]
    clusters.append(current_dts)

    cluster_labels = np.array([-1] * n)
    ctr = 0
    for i, cluster in enumerate(clusters):
        nc = len(cluster)
        cluster_labels[ctr : ctr + nc] = i
        ctr += nc
    return cluster_labels


class _DividendRepairRunner:
    def __init__(self, price_history, df, interval, currency):
        self.price_history = price_history
        self.df = df
        self.interval = interval
        self.currency = currency
        self.logger = utils.get_yf_logger()
        self.log_extras = {
            "yf_cat": "div-adjust-repair-bad",
            "yf_interval": interval,
            "yf_symbol": price_history.ticker,
        }
        history_currency = price_history.get_history_metadata_value("currency")
        self.currency_divide = 1000 if history_currency == "KWF" else 100
        self.too_big_check_threshold = 0.035
        self.df2 = pd.DataFrame()
        self.df2_nan = pd.DataFrame()
        self.df_modified = False
        self.div_indices = np.array([], dtype=int)
        self.div_status_df = pd.DataFrame()
        self.checks = []
        self.div_repairs = {}
    def repair(self):
        """Execute the dividend repair workflow."""
        return self.run()
    def run(self):
        """Run the staged dividend repair pipeline."""
        if self._should_skip():
            return self.df

        if not self._prepare_frames():
            return self.df

        self._repair_double_adjustments()
        self._analyze_dividends()
        if self.div_status_df.empty:
            return self._finalize_if_modified()

        self._add_adjustment_flags()
        self._remove_phantom_dividends()
        self._infer_small_dividends()
        if not self._has_failed_checks():
            return self._finalize_if_modified()

        self._analyze_price_relationships()
        self._prune_inactive_checks()
        self._validate_clusters()
        self._mark_adj_too_small()
        self._expand_combined_flags()
        self._filter_failed_rows()
        if self.div_status_df.empty:
            return self._merge_frames()

        self._apply_repairs()
        self._log_repairs()
        return self._merge_frames()
    def _should_skip(self):
        if self.df is None or self.df.empty:
            return True
        if self.interval in ["1wk", "1mo", "3mo", "1y"]:
            return True
        if "Capital Gains" not in self.df.columns:
            return False
        return bool((self.df["Capital Gains"] > 0).any())
    def _prepare_frames(self):
        f_div = (self.df["Dividends"] != 0.0).to_numpy()
        if not f_div.any():
            self.logger.debug("No dividends to check", extra=self.log_extras)
            return False

        self.df = self.df.sort_index()
        self.df2 = self.df.copy()
        if "Repaired?" not in self.df2.columns:
            self.df2["Repaired?"] = False

        f_nan = self.df2["Close"].isna().to_numpy()
        self.df2_nan = self.df2[f_nan].copy()
        self.df2 = self.df2[~f_nan].copy()

        f_div = (self.df2["Dividends"] != 0.0).to_numpy()
        if not f_div.any():
            self.logger.debug("No dividends to check", extra=self.log_extras)
            return False

        self.div_indices = np.where(f_div)[0]
        return True
    def _repair_double_adjustments(self):
        fixed_dates = []
        for div_idx in reversed(self.div_indices):
            if div_idx == 0:
                continue
            prices_before = self.df2.iloc[div_idx - 1]
            diff = prices_before["Low"] - prices_before["Close"]
            div = self.df2["Dividends"].iloc[div_idx]
            if not 0 < diff < 1.01 * div:
                continue
            dt_before = self.df2.index[div_idx - 1]
            new_close = prices_before["Close"] + div
            if not prices_before["Low"] <= new_close <= prices_before["High"]:
                continue
            self.df2.loc[dt_before, "Close"] = new_close
            adj_after = self.df2["Adj Close"].iloc[div_idx] / self.df2["Close"].iloc[div_idx]
            adj = adj_after * (1.0 - div / self.df2["Close"].iloc[div_idx - 1])
            self.df2.loc[dt_before, "Adj Close"] = self.df2["Close"].iloc[div_idx - 1] * adj
            self.df2.loc[dt_before, "Repaired?"] = True
            self.df_modified = True
            div_dt = cast(pd.Timestamp, self.df2.index[div_idx])
            fixed_dates.append(div_dt.date())

        if fixed_dates:
            msg = f"Repaired double-adjustment on div days {[str(d) for d in fixed_dates]}"
            self.logger.info(msg, extra=self.log_extras)
    def _analyze_dividends(self):
        rows = []
        for div_idx in reversed(self.div_indices):
            status = self._build_dividend_status(div_idx)
            if status is not None:
                rows.append(status)
        if rows:
            self.div_status_df = pd.DataFrame(rows).set_index("date").sort_index()
            self.checks = [
                column for column in self.div_status_df.columns if column.startswith("div_")
            ]
    def _build_dividend_status(self, div_idx):
        if div_idx == 0:
            return None
        dt = self.df2.index[div_idx]
        div = self.df2["Dividends"].iloc[div_idx]
        drop, div_pct = self._calc_primary_drop(div_idx, dt, div)
        drops, drop_2d_max = self._calc_drop_window(div_idx, drop)
        typical_volatility = self._calc_typical_volatility(div_idx)
        context = {
            "idx": div_idx,
            "dt": dt,
            "div": div,
            "div_pct": div_pct,
            "drop": drop,
            "drop_2d_max": drop_2d_max,
            "typical_volatility": typical_volatility,
            "drops": drops,
        }
        possibilities = self._collect_possibilities(context)
        return self._status_row(context, possibilities)
    def _calc_primary_drop(self, div_idx, dt, div):
        prev_close = self.df2["Close"].iloc[div_idx - 1]
        low = self.df2["Low"].iloc[div_idx]
        div_pct = div / prev_close
        if isclose(low, prev_close * 100, rel_tol=0.025):
            return self._adjust_scaled_low(dt, div_idx, div, 100)
        if isclose(low, prev_close * 0.01, rel_tol=0.025):
            return self._adjust_scaled_low(dt, div_idx, div, 0.01)
        return prev_close - low, div_pct
    def _adjust_scaled_low(self, dt, div_idx, div, scale):
        prev_close = self.df2["Close"].iloc[div_idx - 1]
        scaled_close = prev_close * scale
        drop_c2l = scaled_close - self.df2["Low"].iloc[div_idx]
        div_pct = div / scaled_close
        true_adjust = 1.0 - div / (prev_close * 100)
        present_adj = self.df2["Adj Close"].iloc[div_idx - 1] / prev_close
        if not isclose(present_adj, true_adjust, rel_tol=0.025):
            enddt = dt - _datetime.timedelta(seconds=1)
            self.df2.loc[:enddt, "Adj Close"] = true_adjust * self.df2["Close"].loc[:enddt]
            self.df2.loc[:enddt, "Repaired?"] = True
            self.df_modified = True
        return drop_c2l, div_pct
    def _calc_drop_window(self, div_idx, drop):
        if div_idx < len(self.df2) - 1:
            next_drop = self.df2["Close"].iloc[div_idx] - self.df2["Low"].iloc[div_idx + 1]
            drops = np.array([drop, next_drop])
            return drops, np.max(drops)
        drops = np.array([drop])
        return drops, drop
    def _calc_typical_volatility(self, div_idx):
        if (len(self.df2) - div_idx) < 4:
            end = min(len(self.df2), div_idx + 4)
            start = max(0, end - 8)
        else:
            start = max(0, div_idx - 4)
            end = min(len(self.df2), start + 8)
        if end - start < 4:
            return np.nan
        diffs = (
            self.df2["Close"].iloc[start : end - 1].to_numpy()
            - self.df2["Low"].iloc[start + 1 : end].to_numpy()
        )
        return np.mean(np.abs(diffs))
    def _collect_possibilities(self, context):
        if (context["drops"] == 0.0).all() and self.df2["Volume"].iloc[context["idx"]] == 0:
            return self._zero_volume_possibilities(context["div_pct"])
        return self._drop_based_possibilities(context)
    def _zero_volume_possibilities(self, div_pct):
        possibilities = []
        pct_zero_vol = np.sum(self.df2["Volume"] == 0.0) / len(self.df2)
        if div_pct * 100 < 0.1:
            possibilities.append({"state": "div-too-small", "diff": 0.0})
        elif (pct_zero_vol > 0.75 and div_pct > 0.25) or (div_pct > 1.0):
            possibilities.append({"state": "div-too-big", "diff": 0.0})
        return possibilities
    def _drop_based_possibilities(self, context):
        possibilities = []
        split = self.df2["Stock Splits"].loc[context["dt"]]
        div_post_split = None if split == 0.0 else context["div"] / split
        metrics = {
            "div": context["div"],
            "div_pct": context["div_pct"],
            "div_post_split": div_post_split,
            "drop": context["drop"],
            "drop_2d_max": context["drop_2d_max"],
            "typical_volatility": context["typical_volatility"],
        }
        self._append_pre_split_possibility(possibilities, metrics)
        self._append_too_big_possibilities(possibilities, metrics)
        self._append_too_small_possibilities(possibilities, metrics)
        return possibilities
    def _append_pre_split_possibility(self, possibilities, metrics):
        div_post_split = metrics["div_post_split"]
        if div_post_split is None:
            return
        candidate_drop = (
            metrics["drop"] - metrics["typical_volatility"]
            if div_post_split > metrics["div"]
            else metrics["drop_2d_max"]
        )
        if candidate_drop <= 0:
            return
        diff = abs(metrics["div"] - candidate_drop)
        diff_post_split = abs(div_post_split - candidate_drop)
        if diff_post_split * 2 <= diff:
            possibilities.append({"state": "div-pre-split", "diff": diff_post_split})
    def _append_too_big_possibilities(self, possibilities, metrics):
        if metrics["div_pct"] <= self.too_big_check_threshold:
            return
        if metrics["drop_2d_max"] <= 0.0:
            possibilities.append({"state": "div-too-big", "diff": 0.0})
            return
        diff = abs(metrics["div"] - metrics["drop_2d_max"])
        diff_fx = abs((metrics["div"] / self.currency_divide) - metrics["drop_2d_max"])
        if metrics["div_post_split"] is None:
            if diff_fx * 2 <= diff:
                possibilities.append({"state": "div-too-big", "diff": diff_fx})
            return
        diff_fx_post_split = abs(
            (metrics["div_post_split"] / self.currency_divide) - metrics["drop_2d_max"]
        )
        if diff_fx < diff_fx_post_split:
            if diff_fx * 2 <= diff:
                possibilities.append({"state": "div-too-big", "diff": diff_fx})
            return
        if diff_fx_post_split * 2 <= diff:
            possibilities.append({"state": "div-too-big-and-pre-split", "diff": diff_fx_post_split})
    def _append_too_small_possibilities(self, possibilities, metrics):
        if np.isnan(metrics["typical_volatility"]):
            return
        drop_wo_vol = metrics["drop"] - metrics["typical_volatility"]
        if drop_wo_vol <= 0:
            return
        diff = abs(metrics["div"] - drop_wo_vol)
        diff_fx = abs((metrics["div"] * self.currency_divide) - drop_wo_vol)
        if metrics["div_post_split"] is None:
            if diff_fx <= diff:
                possibilities.append({"state": "div-too-small", "diff": diff_fx})
            return
        diff_fx_post_split = abs(
            (metrics["div_post_split"] * self.currency_divide) - drop_wo_vol
        )
        if diff_fx < diff_fx_post_split:
            if diff_fx * 2 <= diff:
                possibilities.append({"state": "div-too-small", "diff": diff_fx})
            return
        if diff_fx_post_split * 2 <= diff:
            possibilities.append(
                {"state": "div-too-small-and-pre-split", "diff": diff_fx_post_split}
            )
    def _status_row(self, context, possibilities):
        div_status = {
            "date": context["dt"],
            "idx": context["idx"],
            "div": context["div"],
            "%": context["div_pct"],
            "drop": context["drop"],
            "drop_2Dmax": context["drop_2d_max"],
            "volume": self.df2["Volume"].iloc[context["idx"]],
            "vol": context["typical_volatility"],
            "div_too_big": False,
            "div_too_small": False,
            "div_pre_split": False,
            "div_too_big_and_pre_split": False,
            "div_too_small_and_pre_split": False,
        }
        if possibilities:
            possibility = sorted(possibilities, key=lambda item: item["diff"])[0]
            div_status[possibility["state"].replace("-", "_")] = True
        return div_status
    def _add_adjustment_flags(self):
        for i in range(len(self.div_status_df)):
            div_idx = self.div_status_df["idx"].iloc[i]
            if div_idx == 0:
                continue
            dt = self.div_status_df.index[i]
            div = self.div_status_df["div"].iloc[i]
            div_pct = div / self.df2["Close"].iloc[div_idx - 1]
            pre_adj = self.df2["Adj Close"].iloc[div_idx - 1] / self.df2["Close"].iloc[div_idx - 1]
            post_adj = self.df2["Adj Close"].iloc[div_idx] / self.df2["Close"].iloc[div_idx]
            div_missing = post_adj == pre_adj
            present_adj = pre_adj / post_adj
            implied_div_yield = 1.0 - present_adj
            div_exceeds_adj = (implied_div_yield < (0.1 * div_pct)) and (not div_missing)
            div_status = {
                "present adj": present_adj,
                "adj_missing": div_missing,
                "adj_exceeds_div": implied_div_yield > (10 * div_pct),
                "div_exceeds_adj": div_exceeds_adj,
            }
            self._update_status_row(dt, div_status)
        self.checks += ["adj_missing", "adj_exceeds_div", "div_exceeds_adj"]
    def _update_status_row(self, dt, values):
        for key, value in values.items():
            if key not in self.div_status_df:
                self.div_status_df[key] = self._empty_status_column(key, value)
            self.div_status_df.loc[dt, key] = value
    def _empty_status_column(self, key, value):
        if isinstance(value, (bool, np.bool_)):
            return False
        if isinstance(value, int):
            return 0
        if isinstance(value, float):
            return 0.0
        if key == "div_true_date":
            return pd.Series(dtype="datetime64[ns, UTC]")
        raise ValueError(key, value, type(value))
    def _remove_phantom_dividends(self):
        self.div_status_df["phantom"] = False
        self._mark_proximate_phantoms()
        self._mark_ratio_phantoms()
        self.checks.append("phantom")
        self._apply_phantom_removals()
    def _mark_proximate_phantoms(self):
        flagged = cast(
            pd.Series,
            self.div_status_df[["div_too_big", "div_exceeds_adj"]].any(axis=1),
        )
        if (not flagged.any()) or len(self.div_status_df) <= 1:
            return
        dts_to_check = self.div_status_df.index[flagged]
        for index in np.where(flagged)[0]:
            div = self.div_status_df.iloc[index]
            other_div = (
                self.div_status_df.iloc[index - 1]
                if index > 0
                else self.div_status_df.iloc[index + 1]
            )
            phantom_dt = self._phantom_from_neighbor(div, other_div, dts_to_check)
            if phantom_dt is not None:
                self._clear_check_flags(phantom_dt)
    def _phantom_from_neighbor(self, div, other_div, dts_to_check):
        div_dt = div.name
        ratio1 = (div["div"] / self.currency_divide) / other_div["div"]
        ratio2 = div["div"] / other_div["div"]
        divergence = min(abs(ratio1 - 1.0), abs(ratio2 - 1.0))
        if abs(div_dt - other_div.name) > _datetime.timedelta(days=17):
            return None
        if other_div["phantom"] or divergence >= 0.01:
            return None
        if other_div.name in dts_to_check:
            return other_div.name if div["drop"] > 1.5 * other_div["drop"] else div_dt
        return div_dt
    def _mark_ratio_phantoms(self):
        self.div_status_df = self.div_status_df.sort_index()
        for i in range(1, len(self.div_status_df)):
            div = self.div_status_df.iloc[i]
            last_div = self.div_status_df.iloc[i - 1]
            if self._should_mark_ratio_phantom(div, last_div):
                phantom_dt = last_div.name if div["drop"] > 1.5 * last_div["drop"] else div.name
                self._clear_check_flags(phantom_dt)
    def _should_mark_ratio_phantom(self, div, last_div):
        if abs(div.name - last_div.name) > _datetime.timedelta(days=17):
            return False
        if last_div["phantom"] or div["phantom"]:
            return False
        ratio = div["div"] / last_div["div"]
        return abs(ratio - 1.0) < 0.08
    def _clear_check_flags(self, dt):
        self.div_status_df.loc[dt, "phantom"] = True
        for column in self.checks:
            if column in self.div_status_df.columns:
                self.div_status_df.loc[dt, column] = False
    def _apply_phantom_removals(self):
        if "phantom" not in self.div_status_df.columns:
            return
        other_checks = [column for column in self.checks if column != "phantom"]
        f_phantom = self.div_status_df["phantom"] & (~self.div_status_df[other_checks].any(axis=1))
        if f_phantom.any():
            self._remove_flagged_phantoms(f_phantom)
        self.div_status_df = self.div_status_df.drop("phantom", axis=1)
        if "phantom" in self.checks:
            self.checks.remove("phantom")
    def _remove_flagged_phantoms(self, f_phantom):
        div_dts = self.div_status_df.index[f_phantom]
        msg = f"Removing phantom div(s): {[str(dt.date()) for dt in div_dts]}"
        self.logger.info(msg, extra=self.log_extras)
        for dt in div_dts:
            enddt = dt - _datetime.timedelta(seconds=1)
            present_adj = self.div_status_df["present adj"].loc[dt]
            self.df2.loc[:enddt, "Adj Close"] /= present_adj
            self.df2.loc[:enddt, "Repaired?"] = True
            self.df2_nan.loc[:enddt, "Adj Close"] /= present_adj
            self.df2_nan.loc[:enddt, "Repaired?"] = True
            self.df2.loc[dt, "Dividends"] = 0
            self.df_modified = True
            self.div_status_df = self.div_status_df.drop(dt)
    def _infer_small_dividends(self):
        if self._has_failed_checks() or len(self.div_status_df) <= 1:
            return
        for i in range(len(self.div_status_df)):
            r_pre, r_post = self._neighbor_ratio_pair(i)
            if r_pre is None or r_post is None:
                continue
            if abs(r_pre - self.currency_divide) < 20 and abs(r_post - self.currency_divide) < 20:
                div_dt = self.div_status_df.index[i]
                self.div_status_df.loc[div_dt, "div_too_small"] = True
    def _neighbor_ratio_pair(self, index):
        r_pre = None
        r_post = None
        if index > 0:
            r_pre = self.div_status_df["%"].iloc[index - 1] / self.div_status_df["%"].iloc[index]
        if index < len(self.div_status_df) - 1:
            r_post = self.div_status_df["%"].iloc[index + 1] / self.div_status_df["%"].iloc[index]
        if r_pre is None:
            r_pre = r_post
        if r_post is None:
            r_post = r_pre
        return r_pre, r_post
    def _has_failed_checks(self):
        return bool(cast(pd.Series, self.div_status_df[self.checks].any(axis=1)).any())
    def _analyze_price_relationships(self):
        for i in range(len(self.div_status_df)):
            div_idx = self.div_status_df["idx"].iloc[i]
            if div_idx == 0:
                continue
            dt = self.div_status_df.index[i]
            row = self._price_relationship_status(div_idx, dt)
            self._update_status_row(dt, row)
        if (
            "div_too_big" in self.div_status_df.columns
            and "div_date_wrong" in self.div_status_df.columns
        ):
            mask = self.div_status_df["div_date_wrong"].to_numpy()
            self.div_status_df.loc[mask, "div_too_big"] = False
        self.checks += ["adj_exceeds_prices", "div_date_wrong"]
    def _price_relationship_status(self, div_idx, dt):
        div = self.div_status_df.loc[dt, "div"]
        div_pct = div / self.df2["Close"].iloc[div_idx - 1]
        lookback_idx, lookahead_idx = self._relationship_window(div_idx, dt)
        div_adj_exceeds_prices = False
        div_date_wrong = False
        div_true_date = pd.NaT
        if lookahead_idx > lookback_idx:
            div_adj_exceeds_prices, div_date_wrong, div_true_date = self._scan_adjustment_reversals(
                dt,
                div,
                div_pct,
                (lookback_idx, lookahead_idx),
            )
        if div_adj_exceeds_prices and self.div_status_df.loc[dt, "div_exceeds_adj"]:
            div_adj_exceeds_prices = False
        if div_adj_exceeds_prices:
            self._maybe_flag_pre_split(dt, div)
        return {
            "adj_exceeds_prices": div_adj_exceeds_prices,
            "div_date_wrong": div_date_wrong,
            "div_true_date": div_true_date,
        }
    def _relationship_window(self, div_idx, dt):
        lookahead_date = dt + _datetime.timedelta(days=35)
        lookahead_idx = bisect.bisect_left(self.df2.index, lookahead_date)
        lookahead_idx = min(lookahead_idx, len(self.df2) - 1)
        lookback_idx = max(0, div_idx - 14)
        future_changes = self.df2["Close"].iloc[div_idx : lookahead_idx + 1].pct_change()
        f_big_change = (future_changes > 2).to_numpy() | (future_changes < -0.9).to_numpy()
        if f_big_change.any():
            lookahead_idx = div_idx + np.where(f_big_change)[0][0] - 1
        return lookback_idx, lookahead_idx
    def _scan_adjustment_reversals(self, dt, div, div_pct, window):
        lookback_idx, lookahead_idx = window
        x = self.df2.iloc[lookback_idx : lookahead_idx + 1].copy()
        x["Adj"] = x["Adj Close"] / x["Close"]
        x["Adj Low"] = x["Adj"] * x["Low"]
        x["delta"] = np.append(
            [0.0],
            x["Low"].iloc[1:].to_numpy() - x["Close"].iloc[:-1].to_numpy(),
        )
        x["adjDelta"] = np.append(
            [0.0],
            x["Adj Low"].iloc[1:].to_numpy()
            - x["Adj Close"].iloc[:-1].to_numpy(),
        )
        if not 0.05 < div_pct < 1.0:
            return False, False, pd.NaT
        adj_div = div * x["Adj"].iloc[0]
        flagged = x["adjDelta"] > (adj_div * 0.6)
        if not flagged.any():
            return False, False, pd.NaT
        for idx in np.where(flagged)[0]:
            status = self._scan_reversal_candidate(dt, x, idx, adj_div)
            if status is not None:
                return status
        return False, False, pd.NaT
    def _scan_reversal_candidate(self, dt, frame, idx, adj_div):
        adj_delta_drop = frame["adjDelta"].iloc[idx]
        if adj_delta_drop <= 1.001 * frame["delta"].iloc[idx]:
            return None
        ratios = (-1 * frame["adjDelta"]) / adj_delta_drop
        f_near1_or_above = ratios >= 0.8
        split = self.df2["Stock Splits"].loc[dt]
        pre_split = self.div_status_df["div_pre_split"].loc[dt]
        if (split == 0.0 or (not pre_split)) and f_near1_or_above.any():
            reversal_idx = self._best_reversal_index(ratios, dt, f_near1_or_above)
            return False, True, ratios.index[reversal_idx]
        if adj_delta_drop > 0.39 * adj_div and (frame["Adj"] < 1.0).any():
            return True, False, pd.NaT
        return None
    def _best_reversal_index(self, ratios, dt, f_near1_or_above):
        near_indices = np.where(f_near1_or_above)[0]
        if len(near_indices) == 1:
            return near_indices[0]
        penalties = np.zeros(len(near_indices))
        for penalty_index, idx2 in enumerate(near_indices):
            dti = ratios.index[idx2]
            penalties[penalty_index] += (dt - dti).days if dti < dt else 0.1 * (dti - dt).days
        return near_indices[int(np.argmin(penalties))]
    def _maybe_flag_pre_split(self, dt, div):
        split = self.df2["Stock Splits"].loc[dt]
        if split == 0.0:
            return
        div_post_split = div / split
        if div_post_split > div:
            candidate_drop = (
                self.div_status_df["drop"].loc[dt] - self.div_status_df["vol"].loc[dt]
            )
        else:
            candidate_drop = self.div_status_df["drop_2Dmax"].loc[dt]
        if candidate_drop <= 0:
            return
        diff = abs(div - candidate_drop)
        diff_post_split = abs(div_post_split - candidate_drop)
        if diff_post_split <= (diff * 1.1):
            self.div_status_df.loc[dt, "div_pre_split"] = True
    def _prune_inactive_checks(self):
        for column in list(self.checks):
            if not bool(cast(pd.Series, self.div_status_df[column]).any()):
                self.div_status_df = self.div_status_df.drop(column, axis=1)
        if (
            "div_true_date" in self.div_status_df.columns
            and bool(cast(pd.Series, self.div_status_df["div_true_date"]).isna().all())
        ):
            self.div_status_df = self.div_status_df.drop("div_true_date", axis=1)
        self.checks = [column for column in self.checks if column in self.div_status_df.columns]
    def _validate_clusters(self):
        self.div_status_df = self.div_status_df.sort_values("%")
        self.div_status_df["cluster"] = _cluster_dividends(self.div_status_df, column="%")
        for cid in self.div_status_df["cluster"].unique():
            fc = self.div_status_df["cluster"] == cid
            cluster = self.div_status_df[fc].sort_index()
            cluster_checks = self._active_cluster_checks(cluster)
            for column in cluster_checks:
                self._apply_cluster_rule(fc, cluster, column)
    def _active_cluster_checks(self, cluster):
        cluster_checks = []
        for column in self.checks:
            cluster_col = cast(pd.Series, cluster[column])
            if bool(cluster_col.to_numpy().any()):
                cluster_checks.append(column)
        return cluster_checks
    def _apply_cluster_rule(self, fc, cluster, column):
        n = len(cluster)
        f_fail = cast(np.ndarray, cast(pd.Series, cluster[column]).to_numpy())
        n_fail = np.sum(f_fail)
        if n_fail in [0, n]:
            return
        pct_fail = n_fail / n
        if column == "div_too_big":
            self._validate_too_big_cluster(fc, cluster, f_fail, pct_fail)
            return
        if column == "div_too_small":
            self._validate_too_small_cluster(fc, cluster, pct_fail)
            return
        if self._should_skip_cluster_column(column, cluster, n_fail):
            return
    def _validate_too_big_cluster(self, fc, cluster, f_fail, pct_fail):
        true_threshold = 1.0
        fals_threshold = 0.25
        if (
            "div_date_wrong" in cluster.columns
            and (cluster["div_too_big"] == cluster["div_date_wrong"]).all()
        ):
            return
        if (
            "adj_exceeds_prices" in cluster.columns
            and (
                cluster["div_too_big"]
                == (cluster["div_too_big"] & cluster["adj_exceeds_prices"])
            ).all()
        ):
            f_adj_exceeds_prices = cast(
                np.ndarray,
                cast(pd.Series, cluster["adj_exceeds_prices"]).to_numpy(),
            )
            n = np.sum(f_adj_exceeds_prices)
            pct_fail = np.sum(f_fail[f_adj_exceeds_prices]) / n
            if pct_fail > 0.5:
                mask = fc & self.div_status_df["adj_exceeds_prices"].to_numpy()
                self.div_status_df.loc[mask, "div_too_big"] = True
            return
        if "div_exceeds_adj" in cluster.columns and bool(
            cast(pd.Series, cluster["div_exceeds_adj"]).all()
        ):
            if (cluster.loc[f_fail, "vol"] == 0).all():
                fals_threshold = 2 / 3
            else:
                true_threshold = 0.25
        elif "adj_exceeds_prices" in cluster.columns and (
            cluster["div_too_big"] == cluster["adj_exceeds_prices"]
        ).all():
            true_threshold = 0.5
        else:
            fals_threshold = 0.5
        thresholds = {"true": true_threshold, "false": fals_threshold}
        self._apply_cluster_thresholds(fc, pct_fail, thresholds, "div_too_big")
    def _validate_too_small_cluster(self, fc, cluster, pct_fail):
        true_threshold = 1.0
        fals_threshold = 0.1
        if "adj_exceeds_div" not in cluster.columns:
            true_threshold = 6 / 11
            fals_threshold = 0.5
        thresholds = {"true": true_threshold, "false": fals_threshold}
        self._apply_cluster_thresholds(fc, pct_fail, thresholds, "div_too_small")
    def _apply_cluster_thresholds(self, fc, pct_fail, thresholds, column):
        if pct_fail >= thresholds["true"]:
            self.div_status_df.loc[fc, column] = True
            if column == "div_too_big" and "div_date_wrong" in self.div_status_df.columns:
                self.div_status_df.loc[fc, "div_date_wrong"] = False
                self.div_status_df.loc[fc, "div_true_date"] = pd.NaT
            return
        if pct_fail <= thresholds["false"]:
            self.div_status_df.loc[fc, column] = False
    def _should_skip_cluster_column(self, column, cluster, n_fail):
        if (
            column == "adj_missing"
            and bool(cast(pd.Series, cluster[column]).iloc[-1])
            and n_fail == 1
        ):
            return True
        if column in ["div_exceeds_adj", "adj_exceeds_prices"]:
            return True
        if column == "phantom" and self.price_history.ticker in ["KAP.IL", "SAND"]:
            return True
        return column in ["div_date_wrong", "div_pre_split", "div_too_big_and_pre_split"]
    def _mark_adj_too_small(self):
        if "div_too_big" not in self.checks or "div_exceeds_adj" not in self.checks:
            return
        self.div_status_df["adj_too_small"] = False
        for i in range(len(self.div_status_df)):
            dt = self.div_status_df.index[i]
            row = self.div_status_df.iloc[i]
            if not (row["div_too_big"] and row["div_exceeds_adj"]):
                continue
            close = row["div"] / row["%"]
            implied_div_yield = (1 - row["present adj"]) * close
            ratio = row["div"] / implied_div_yield
            if abs(ratio - (self.currency_divide * self.currency_divide)) < self.currency_divide:
                self.div_status_df.loc[dt, "adj_too_small"] = True
        if bool(cast(pd.Series, self.div_status_df["adj_too_small"]).any()):
            self.checks.append("adj_too_small")
        else:
            self.div_status_df = self.div_status_df.drop("adj_too_small", axis=1)
    def _expand_combined_flags(self):
        if "div_too_big_and_pre_split" not in self.div_status_df.columns:
            return
        for column in ["div_too_big", "div_pre_split"]:
            if column in self.div_status_df:
                self.div_status_df[column] = (
                    self.div_status_df[column]
                    | self.div_status_df["div_too_big_and_pre_split"]
                )
            else:
                self.div_status_df[column] = self.div_status_df["div_too_big_and_pre_split"]
                self.checks.append(column)
        self.div_status_df = self.div_status_df.drop("div_too_big_and_pre_split", axis=1)
        self.checks.remove("div_too_big_and_pre_split")
    def _filter_failed_rows(self):
        self.div_status_df = self.div_status_df.sort_index()
        self.div_status_df = self.div_status_df[self.div_status_df[self.checks].any(axis=1)]
    def _apply_repairs(self):
        for cid in list(cast(pd.Series, self.div_status_df["cluster"]).unique()):
            cluster = cast(
                pd.DataFrame,
                self.div_status_df[self.div_status_df["cluster"] == cid].sort_index(
                    ascending=False
                ),
            )
            cluster["Fixed?"] = False
            for i in range(len(cluster) - 1, -1, -1):
                dt = cluster.index[i]
                row = cluster.iloc[i]
                flags = self._repair_flags(row)
                self._normalize_repair_flags(cluster, dt, flags)
                if self._apply_single_check_repair(cluster, dt, row, flags):
                    continue
                if self._apply_two_check_repair(cluster, dt, row, flags):
                    continue
                self._apply_three_check_repair(cluster, dt, row, flags)
    def _repair_flags(self, row):
        return {
            "adj_missing": "adj_missing" in row and row["adj_missing"],
            "div_exceeds_adj": "div_exceeds_adj" in row and row["div_exceeds_adj"],
            "adj_exceeds_div": "adj_exceeds_div" in row and row["adj_exceeds_div"],
            "adj_exceeds_prices": "adj_exceeds_prices" in row and row["adj_exceeds_prices"],
            "div_too_small": "div_too_small" in row and row["div_too_small"],
            "div_too_big": "div_too_big" in row and row["div_too_big"],
            "div_pre_split": "div_pre_split" in row and row["div_pre_split"],
            "div_date_wrong": "div_date_wrong" in row and row["div_date_wrong"],
            "adj_too_small": "adj_too_small" in row and row["adj_too_small"],
            "n_failed_checks": np.sum([row[column] for column in self.checks if column in row]),
        }
    def _normalize_repair_flags(self, cluster, dt, flags):
        if flags["div_too_big"] and flags["adj_exceeds_prices"] and flags["n_failed_checks"] == 2:
            flags["adj_exceeds_prices"] = False
            flags["n_failed_checks"] -= 1
        if flags["div_date_wrong"]:
            self._clear_row_flag(cluster, dt, flags, "div_too_big")
            self._clear_row_flag(cluster, dt, flags, "div_exceeds_adj")
        if flags["div_pre_split"] and flags["adj_exceeds_prices"]:
            self._clear_row_flag(cluster, dt, flags, "adj_exceeds_prices")
    def _clear_row_flag(self, cluster, dt, flags, column):
        if flags[column]:
            flags[column] = False
            cluster.loc[dt, column] = False
            flags["n_failed_checks"] -= 1
    def _apply_single_check_repair(self, cluster, dt, row, flags):
        if flags["n_failed_checks"] != 1:
            return False
        repair_case = None
        if flags["div_exceeds_adj"] or flags["adj_exceeds_div"]:
            repair_case = (
                "too-small div-adjust"
                if flags["div_exceeds_adj"]
                else "too-big div-adjust"
            )
        elif flags["div_too_small"]:
            repair_case = "div_too_small"
        elif flags["div_too_big"]:
            repair_case = "div_too_big"
        elif flags["adj_missing"]:
            repair_case = "adj_missing"
        elif flags["div_date_wrong"]:
            repair_case = "div_date_wrong"
        elif flags["adj_exceeds_prices"]:
            repair_case = "adj_exceeds_prices"
        elif flags["div_pre_split"]:
            repair_case = "div_pre_split"

        if repair_case is None:
            return False
        return self._execute_single_check_repair(repair_case, cluster, dt, row)
    def _execute_single_check_repair(self, repair_case, cluster, dt, row):
        handled = True
        if repair_case in ["too-small div-adjust", "too-big div-adjust"]:
            adj_correction = (1.0 - row["%"]) / row["present adj"]
            self._record_adjustment_repair(cluster, dt, repair_case, adj_correction)
        elif repair_case == "div_too_small":
            self._repair_too_small_div(cluster, dt, row)
        elif repair_case == "div_too_big":
            self._repair_too_big_div(cluster, dt, row)
        elif repair_case == "adj_missing":
            self._record_adjustment_repair(
                cluster,
                dt,
                "missing div-adjust",
                1.0 - row["%"],
            )
        elif repair_case == "div_date_wrong":
            self._repair_wrong_dividend_date(cluster, dt, row)
        elif repair_case == "adj_exceeds_prices":
            handled = self._repair_adj_exceeds_prices(cluster, dt, row)
        elif repair_case == "div_pre_split":
            self._repair_pre_split_div(cluster, dt, row)
        else:
            handled = False
        return handled
    def _apply_two_check_repair(self, cluster, dt, row, flags):
        if flags["n_failed_checks"] != 2:
            return False
        if flags["div_too_big"] and flags["adj_missing"]:
            self._repair_big_div_missing_adjust(cluster, dt, row)
            return True
        if flags["div_too_big"] and flags["div_exceeds_adj"]:
            current_dividend = cast(float, self.df2.loc[dt, "Dividends"])
            self.df2.loc[dt, "Dividends"] = current_dividend / self.currency_divide
            self._record_repair("div-too-big", dt)
            cluster.loc[dt, "Fixed?"] = True
            self.df_modified = True
            return True
        if flags["div_too_big"] and flags["adj_exceeds_prices"]:
            self._repair_big_div_and_adjust(cluster, dt, row)
            return True
        if flags["div_too_small"] and flags["adj_exceeds_div"]:
            self._repair_small_div_and_adjust(cluster, dt, row)
            return True
        return False
    def _apply_three_check_repair(self, cluster, dt, row, flags):
        if flags["n_failed_checks"] != 3:
            return False
        if flags["div_too_big"] and flags["div_exceeds_adj"] and flags["div_pre_split"]:
            self._repair_big_pre_split_div(cluster, dt, row)
            return True
        if flags["div_too_big"] and flags["div_exceeds_adj"] and flags["adj_too_small"]:
            self._repair_big_div_small_adjust(cluster, dt, row)
            return True
        return False
    def _record_adjustment_repair(self, cluster, dt, key, adj_correction):
        self._record_repair(key, dt)
        self._apply_adj_correction(dt, adj_correction)
        cluster.loc[dt, "Fixed?"] = True
    def _repair_too_small_div(self, cluster, dt, row):
        correct_div = row["div"] * self.currency_divide
        self.df2.loc[dt, "Dividends"] = correct_div
        target_adj = 1.0 - ((1.0 - row["present adj"]) * self.currency_divide)
        self._record_repair("too-small div & div-adjust", dt)
        self._apply_adj_correction(dt, target_adj / row["present adj"])
        cluster.loc[dt, "Fixed?"] = True
    def _repair_too_big_div(self, cluster, dt, row):
        correction = 1.0 / self.currency_divide
        self.df2.loc[dt, "Dividends"] = row["div"] * correction
        target_adj = 1.0 - (row["%"] * correction)
        self._record_repair("too-big div & div-adjust", dt)
        self._apply_adj_correction(dt, target_adj / row["present adj"])
        cluster.loc[dt, "Fixed?"] = True
    def _repair_wrong_dividend_date(self, cluster, dt, row):
        enddt = dt - _datetime.timedelta(seconds=1)
        self._record_repair("wrong ex-div date", dt)
        self.df2.loc[:enddt, "Adj Close"] *= 1.0 / row["present adj"]
        self.df2_nan.loc[:enddt, "Adj Close"] *= 1.0 / row["present adj"]
        div_true_date = row["div_true_date"]
        close_before = self.df2["Close"].iloc[row["idx"]]
        true_adj = 1.0 - row["div"] / close_before
        enddt2 = div_true_date - _datetime.timedelta(seconds=1)
        self.df2.loc[:enddt2, "Adj Close"] *= true_adj
        self.df2_nan.loc[:enddt2, "Adj Close"] *= true_adj
        self.df2.loc[div_true_date, "Dividends"] += row["div"]
        self.df2.loc[dt, "Dividends"] = 0
        self.df2.loc[:enddt, "Repaired?"] = True
        self.df2_nan.loc[:enddt, "Repaired?"] = True
        self.df_modified = True
        cluster.loc[dt, "Fixed?"] = True
    def _repair_adj_exceeds_prices(self, cluster, dt, row):
        target_adj = 1.0 - row["%"]
        present_adj = row["present adj"]
        if abs((target_adj / present_adj) - 1) <= 0.05:
            self.div_status_df = self.div_status_df.drop(dt)
            cluster.drop(dt, inplace=True)
            return True
        self._record_repair("adj-exceeds-prices & div-adjust", dt)
        self._apply_adj_correction(dt, target_adj / present_adj)
        cluster.loc[dt, "Fixed?"] = True
        return True
    def _repair_pre_split_div(self, cluster, dt, row):
        correction = 1.0 / self.df2["Stock Splits"].loc[dt]
        self.df2.loc[dt, "Dividends"] = row["div"] * correction
        target_adj = 1.0 - (row["%"] * correction)
        self._record_repair("pre-split div & div-adjust", dt)
        self._apply_adj_correction(dt, target_adj / row["present adj"])
        cluster.loc[dt, "Fixed?"] = True
    def _repair_big_div_missing_adjust(self, cluster, dt, row):
        current_dividend = cast(float, self.df2.loc[dt, "Dividends"])
        self.df2.loc[dt, "Dividends"] = current_dividend / self.currency_divide
        self._record_repair("too-big div and missing div-adjust", dt)
        self._apply_adj_correction(dt, 1.0 - row["%"] / self.currency_divide)
        cluster.loc[dt, "Fixed?"] = True
    def _repair_big_div_and_adjust(self, cluster, dt, row):
        current_dividend = cast(float, self.df2.loc[dt, "Dividends"])
        self.df2.loc[dt, "Dividends"] = current_dividend / self.currency_divide
        target_adj = 1.0 - (row["%"] / self.currency_divide)
        self._record_repair("too-big div & div-adjust", dt)
        self._apply_adj_correction(dt, target_adj / row["present adj"])
        cluster.loc[dt, "Fixed?"] = True
    def _repair_small_div_and_adjust(self, cluster, dt, row):
        current_dividend = cast(float, self.df2.loc[dt, "Dividends"])
        self.df2.loc[dt, "Dividends"] = current_dividend * self.currency_divide
        key = "too-small div"
        if "FX was repaired" in row and row["FX was repaired"]:
            div_adj = 1.0 - (row["%"] * self.currency_divide)
            self._apply_adj_correction(dt, div_adj / row["present adj"])
            key += " and FX mixup"
        else:
            self.df_modified = True
        self._record_repair(key, dt)
        cluster.loc[dt, "Fixed?"] = True
    def _repair_big_pre_split_div(self, cluster, dt, row):
        correction = (1.0 / self.currency_divide) * (
            1.0 / self.df2["Stock Splits"].loc[dt]
        )
        self.df2.loc[dt, "Dividends"] = row["div"] * correction
        target_adj = 1.0 - (row["%"] * correction)
        self._record_repair("too-big div & pre-split & div-adjust", dt)
        self._apply_adj_correction(dt, target_adj / row["present adj"])
        cluster.loc[dt, "Fixed?"] = True
    def _repair_big_div_small_adjust(self, cluster, dt, row):
        close = row["div"] / row["%"]
        div_true = row["div"] / self.currency_divide
        pct_true = div_true / close
        self.df2.loc[dt, "Dividends"] = div_true
        self._record_repair("div-too-big and adj-too-small", dt)
        self._apply_adj_correction(dt, (1.0 - pct_true) / row["present adj"])
        cluster.loc[dt, "Fixed?"] = True
    def _apply_adj_correction(self, dt, adj_correction):
        enddt = dt - _datetime.timedelta(seconds=1)
        self.df2.loc[:enddt, "Adj Close"] *= adj_correction
        self.df2.loc[:enddt, "Repaired?"] = True
        self.df2_nan.loc[:enddt, "Adj Close"] *= adj_correction
        self.df2_nan.loc[:enddt, "Repaired?"] = True
        self.df_modified = True
    def _record_repair(self, key, dt):
        self.div_repairs.setdefault(key, []).append(dt)
    def _log_repairs(self):
        for key, repaired_dts in self.div_repairs.items():
            msg = f"Repaired {key}: {[str(dt.date()) for dt in sorted(repaired_dts)]}"
            self.logger.info(msg, extra=self.log_extras)
    def _finalize_if_modified(self):
        if not self.df_modified:
            return self.df
        return self._merge_frames()
    def _merge_frames(self):
        if not self.df2_nan.empty:
            self.df2 = pd.concat([self.df2, self.df2_nan]).sort_index()
        return self.df2
