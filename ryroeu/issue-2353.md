# Issue #2353

## Title

yfinance does not return the latest (current day during market session) day's data using `max` parameter.

## Current Status

Not resolved

## Reproduction

Reproduced live during active US market hours with repeated calls to:

```python
yf.download(['SPY', 'QQQ'], period='max', group_by='ticker', auto_adjust=True, threads=False)
```

## Current Findings

The failure is intermittent on the multi-ticker `download()` path.

Repeated calls can return today's row with `Volume` populated but `Open`, `High`, `Low`, and `Close` all `NaN` for one ticker.

Single-ticker `period='max'` downloads stayed correct.

Shorter `1y` and `5y` downloads also stayed correct.

## Tracker Update

Tracker status was updated from `needs reproduction` to `not resolved`.