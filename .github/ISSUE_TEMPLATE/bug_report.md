---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

# READ BEFORE POSTING

### Are you up-to-date?

Upgrade to the latest version and confirm the issue/bug is still there.

`$ pip install yfinance --upgrade --no-cache-dir`

Confirm by running:

`import yfinance as yf ; print(yf.__version__)`

and comparing against [PIP](https://pypi.org/project/yfinance/#history).

### Does Yahoo actually have the data?

Visit `finance.yahoo.com` and confim they have your data. Maybe your ticker was delisted.

Then check that you are spelling ticker *exactly* same as Yahoo.

### Are you spamming Yahoo?

Yahoo Finance free service has limit on query rate (roughly 100/s). Them delaying or blocking your spam is not a bug.

### Still think it's a bug?

Delete this default message and submit your bug report here, providing the following as best you can:

- Info about your system:
  - yfinance version
  - operating system
- Simple code that reproduces your problem
- The error message
