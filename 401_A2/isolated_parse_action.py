import pandas as _pd


def parse_actions(data, tz=None):
    dividends = _pd.DataFrame(columns=["Dividends"])
    splits = _pd.DataFrame(columns=["Stock Splits"])

    if "events" in data:
        if "dividends" in data["events"]:
            dividends = _pd.DataFrame(
                data=list(data["events"]["dividends"].values()))
            dividends.set_index("date", inplace=True)
            dividends.index = _pd.to_datetime(dividends.index, unit="s")
            dividends.sort_index(inplace=True)
            if tz is not None:
                dividends.index = dividends.index.tz_localize(tz)

            dividends.columns = ["Dividends"]

        if "splits" in data["events"]:
            splits = _pd.DataFrame(
                data=list(data["events"]["splits"].values()))
            splits.set_index("date", inplace=True)
            splits.index = _pd.to_datetime(splits.index, unit="s")
            splits.sort_index(inplace=True)
            if tz is not None:
                splits.index = splits.index.tz_localize(tz)
            splits["Stock Splits"] = splits["numerator"] / \
                splits["denominator"]
            splits = splits["Stock Splits"]

    return dividends, splits

