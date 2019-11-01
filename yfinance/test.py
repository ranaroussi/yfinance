#sanity check for most common library uses all working
def test_yfinance():
  #stock Microsoft, ETF Russell 2000 Growth,
  #mutual fund Vanguard 500 Index fund,
  #index S&P500, currency BitCoin-US Dollars
  for x in ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']:
    tick = yf.Ticker(x)
    #always should have info and history for valid symbols
    assert(tick.info != None and tick.info != {})
    assert(len(tick.history(period="max", rounding=True)) != 0)
    #following should always gracefully handled, no crashes
    tick.cashflow
    tick.balance_sheet
    tick.financials
    tick.sustainability
