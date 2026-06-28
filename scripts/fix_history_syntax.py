"""Fix syntax errors introduced by automated history.py splits."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"


def dedupe_staticmethods(source: str) -> str:
    return re.sub(
        r"(    @staticmethod\n)+",
        "    @staticmethod\n",
        source,
    )


def fix_contradicts_scan(source: str) -> str:
    marker = "    def _div_adjust_contradicts_scan_loop("
    if "x, idx," in source.split(marker, 1)[1].split("def ", 1)[0]:
        return source
    start = source.index(marker)
    end = source.index("    def _div_adjust_contradicts_pre_split_flag", start)
    block = """    @staticmethod
    def _div_adjust_contradicts_scan_loop(
            df2, div_status_df, dt, div, deltas, adjDiv, x, idx,
            div_adj_exceeds_prices, div_date_wrong, div_true_date):
        adjDelta_drop = deltas['adjDelta'].iloc[idx]
        if adjDelta_drop > 1.001*deltas['delta'].iloc[idx]:
            ratios = (-1*deltas['adjDelta'])/adjDelta_drop
            f_near1_or_above = ratios>=0.8
            split = df2['Stock Splits'].loc[dt]
            pre_split = div_status_df['div_pre_split'].loc[dt]
            if (split==0.0 or (not pre_split)) and f_near1_or_above.any():
                near_indices = np.where(f_near1_or_above)[0]
                if len(near_indices) > 1:
                    penalties = np.zeros(len(near_indices))
                    for i in range(len(near_indices)):
                        idx = near_indices[i]
                        dti = ratios.index[idx]
                        if dti < dt:
                            penalties[i] += (dt-dti).days
                        else:
                            penalties[i] += 0.1*(dti-dt).days
                    i = np.argmin(penalties)
                    reversal_idx = near_indices[i]
                else:
                    reversal_idx = near_indices[0]
                div_date_wrong = True
                div_true_date = ratios.index[reversal_idx]
                return div_adj_exceeds_prices, div_date_wrong, div_true_date
            elif adjDelta_drop > 0.39*adjDiv:
                if (x['Adj']<1.0).any():
                    div_adj_exceeds_prices = True
                return div_adj_exceeds_prices, div_date_wrong, div_true_date
        return div_adj_exceeds_prices, div_date_wrong, div_true_date

    @staticmethod
    def _div_adjust_contradicts_scan_core(
            df2, div_status_df, dt, div, div_idx, div_pct,
            lookahead_idx, lookback_idx,
            div_adj_exceeds_prices, div_date_wrong, div_true_date):
        if lookahead_idx > lookback_idx:
            x = df2.iloc[lookback_idx:lookahead_idx+1].copy()
            x['Adj'] = x['Adj Close'] / x['Close']
            x['Adj Low'] = x['Adj'] * x['Low']
            deltas = x['Low'].iloc[1:].to_numpy() - x['Close'].iloc[:-1].to_numpy()
            deltas = np.append([0.0], deltas)
            x['delta'] = deltas
            adjDeltas = x['Adj Low'].iloc[1:].to_numpy() - x['Adj Close'].iloc[:-1].to_numpy()
            adjDeltas = np.append([0.0], adjDeltas)
            x['adjDelta'] = adjDeltas
            deltas = x[['delta', 'adjDelta']]
            if div_pct > 0.05 and div_pct < 1.0:
                adjDiv = div * x['Adj'].iloc[0]
                f = deltas['adjDelta'] > (adjDiv*0.6)
                if f.any():
                    indices = np.where(f)[0]
                    for idx in indices:
                        div_adj_exceeds_prices, div_date_wrong, div_true_date = (
                            PriceHistory._div_adjust_contradicts_scan_loop(
                                df2, div_status_df, dt, div, deltas, adjDiv, x, idx,
                                div_adj_exceeds_prices, div_date_wrong, div_true_date))
                        if div_date_wrong or div_adj_exceeds_prices:
                            break
        return div_adj_exceeds_prices, div_date_wrong, div_true_date

    @staticmethod
    def _div_adjust_contradicts_scan(df2, div_status_df, i, checks):
        div_idx = div_status_df['idx'].iloc[i]
        dt = div_status_df.index[i]
        div = div_status_df['div'].iloc[i]
        if div_idx == 0:
            return div_status_df, False, False, pd.NaT, dt
        div_pct = div / df2['Close'].iloc[div_idx-1]
        lookahead_date = dt+_datetime.timedelta(days=35)
        lookahead_idx = bisect.bisect_left(df2.index, lookahead_date)
        lookahead_idx = min(lookahead_idx, len(df2)-1)
        lookback_idx = max(0, div_idx-14)
        future_changes = df2['Close'].iloc[div_idx:lookahead_idx+1].pct_change()
        f_big_change = (future_changes > 2).to_numpy() | (future_changes < -0.9).to_numpy()
        if f_big_change.any():
            lookahead_idx = div_idx + np.where(f_big_change)[0][0]-1
        div_adj_exceeds_prices = False
        div_date_wrong = False
        div_true_date = pd.NaT
        div_adj_exceeds_prices, div_date_wrong, div_true_date = (
            PriceHistory._div_adjust_contradicts_scan_core(
                df2, div_status_df, dt, div, div_idx, div_pct,
                lookahead_idx, lookback_idx,
                div_adj_exceeds_prices, div_date_wrong, div_true_date))
        return div_status_df, div_adj_exceeds_prices, div_date_wrong, div_true_date, dt

"""
    return source[:start] + block + source[end:]


def fix_cluster_continue(source: str) -> str:
    for name, replacement in (
        ("_div_adjust_cluster_too_big", "return div_status_df, cluster"),
        ("_div_adjust_cluster_too_small", "return div_status_df"),
        ("_div_adjust_cluster_one_check", "return div_status_df"),
    ):
        start = source.index(f"    def {name}(")
        end = source.index("\n    def ", start + 1)
        chunk = source[start:end]
        if "continue" not in chunk:
            continue
        chunk = re.sub(r"\n                continue\n", f"\n                {replacement}\n", chunk)
        chunk = re.sub(r"\n            continue\n", f"\n            {replacement}\n", chunk)
        source = source[:start] + chunk + source[end:]
    return source


def fix_cluster_loop(source: str) -> str:
    bad = """            cluster_checks = [c for c in checks if c in cluster.columns]

        for c in cluster_checks:
            div_status_df = PriceHistory._div_adjust_cluster_one_check(
                div_status_df, cluster, fc, c, n, checks)"""
    good = """            cluster_checks = [c for c in checks if c in cluster.columns]

            for c in cluster_checks:
                div_status_df = PriceHistory._div_adjust_cluster_one_check(
                    div_status_df, cluster, fc, c, n, checks)"""
    return source.replace(bad, good)


def main() -> None:
    source = HISTORY.read_text(encoding="utf-8")
    for step in (dedupe_staticmethods, fix_contradicts_scan, fix_cluster_continue, fix_cluster_loop):
        source = step(source)
        ast.parse(source)
        print(f"  ok: {step.__name__}")
    HISTORY.write_text(source, encoding="utf-8")
    compile(source, str(HISTORY), "exec")
    print("syntax ok")


if __name__ == "__main__":
    main()
