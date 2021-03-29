## Assignment: Test .earnings and .quarterly_earnings in ticker.py file lines 148 to 154

**Issues:**

\Issue #533

- PR #534 is linked to this issue

\Issue #543

\Issue #489

\Issue #475

- PR #480 is linked to this issue

\Issue #191

\Issue #423

\Issue #400

\Issue #328

\Issue #291

- PR #272 is linked to this issue

\Issue #254

\Issue #223

\Issue #181

- PR #187 is linked to this issue, it is closed
- PR #174 is linked to this issue, it is closed


**Pull Requests**

\PR #620 - [Added an important financialCurrency property](https://github.com/ranaroussi/yfinance/pull/620)

- Changes made to yfinance/base.py
- Returns USD currency for \_get_fundamentals

\PR #534 - [Fix to financials](https://github.com/ranaroussi/yfinance/pull/534)

- Fixes URL datastream
- Change the data source that is processed by earnings function (base.py line \#394 function)

\PR #246 - [Fixes the scrape URL for financial data](https://github.com/ranaroussi/yfinance/pull/246)

- Functionally does the same as issue \#535, however includes test cases

\PR #480 - [Fix the fetching of fundamentals](https://github.com/ranaroussi/yfinance/pull/480) \[Merged]

- Prevents the override of ticker url by holders url
