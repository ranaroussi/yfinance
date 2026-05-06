"""
Fixtures for Yahoo Finance HTML endpoints (BeautifulSoup-parsed pages).
"""

import datetime

from ..response import MockResponse


def earnings_calendar_response(symbol, size=25):
    rows = []
    base_date = datetime.date.today()
    for i in range(size):
        offset_days = (i // 2 + 1) * 90
        if i % 2 == 0:
            d = base_date + datetime.timedelta(days=offset_days)
            eps_est = round(2.10 - i * 0.01, 2)
            reported = "-"
            surprise = "-"
        else:
            d = base_date - datetime.timedelta(days=offset_days)
            eps_est = round(2.10 - i * 0.01, 2)
            reported = round(eps_est * 1.02, 2)
            surprise = round((reported / eps_est - 1) * 100, 2)

        date_str = d.strftime("%B %d, %Y at 4 PM EDT")
        rows.append(
            f"<tr><td>{symbol}</td><td>{symbol} Inc.</td>"
            f"<td>{date_str}</td><td>{eps_est}</td>"
            f"<td>{reported}</td><td>{surprise}</td></tr>"
        )

    html = (
        "<html><body><table>"
        "<thead><tr>"
        "<th>Symbol</th><th>Company</th><th>Earnings Date</th>"
        "<th>EPS Estimate</th><th>Reported EPS</th><th>Surprise (%)</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></body></html>"
    )
    return MockResponse(text=html)


def valuation_response():
    html = """
    <html><body>
    <table>
      <tr><th>Measure</th><th>Current</th><th>12/2023</th><th>12/2022</th></tr>
      <tr><td>Market Cap (intraday)</td><td>2.50T</td><td>2.99T</td><td>2.07T</td></tr>
      <tr><td>Enterprise Value</td><td>2.60T</td><td>3.07T</td><td>2.13T</td></tr>
      <tr><td>Trailing P/E</td><td>28.5</td><td>29.7</td><td>22.3</td></tr>
      <tr><td>Forward P/E</td><td>25.0</td><td>26.1</td><td>19.8</td></tr>
    </table>
    </body></html>
    """
    return MockResponse(text=html)
