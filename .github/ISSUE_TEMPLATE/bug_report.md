---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

# IMPORTANT

If you want help, you got to read this first, follow the instructions.

### Are you up-to-date?

Upgrade to the latest version and confirm the issue/bug is still there.

`$ pip install yfinance --upgrade --no-cache-dir`

Confirm by running:

`import yfinance as yf ; print(yf.__version__)`

and comparing against [PIP](https://pypi.org/project/yfinance/#history).

### Does Yahoo actually have the data?

Are you spelling ticker *exactly* same as Yahoo?

Then visit `finance.yahoo.com` and confirm they have the data you want. Maybe your ticker was delisted, or your expectations of `yfinance` are wrong.

### Are you spamming Yahoo?

Yahoo Finance free service has rate-limiting depending on request type - roughly 60/minute for prices, 10/minute for info. Once limit hit, Yahoo can delay, block, or return bad data. Not a `yfinance` bug.

### Still think it's a bug?

Delete this default message (all of it) and submit your bug report here, providing the following as best you can:

- Simple code that reproduces your problem, that we can copy-paste-run
- Exception message with full traceback, or proof `yfinance` returning bad data
- `yfinance` version and Python version
- Operating system type
