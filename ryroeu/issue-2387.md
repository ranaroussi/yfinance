# Issue #2387

## Title

Project tests errors

## Current Status

Not resolved

## Reproduction

Exact repro command:

```bash
python -m unittest discover -k test
```

This command still fails on the current fork.

## Current Findings

This issue is no longer a vague project-wide test failure report.

The remaining failures are narrowed to four live subfailures in:

`tests.price_repair_assumptions.TestPriceRepairAssumptions.test_resampling`

Observed failing subcases:

- `GOOGL` with `interval='1wk'`, `period='ytd'`
- `GLEN.L` with `interval='1wk'`, `period='ytd'`
- `2330.TW` with `interval='1wk'`, `period='1mo'`
- `2330.TW` with `interval='1wk'`, `period='ytd'`

## Tracker Update

Tracker status was updated from `not addressed` to `not resolved`.