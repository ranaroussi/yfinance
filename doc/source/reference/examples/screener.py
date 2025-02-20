from yfinance.screener import (
    EquityQuery as EqyQy,
    FundQuery as FndQy,
    screen,
    industry,
)

# Set up a query

aggressive_small_caps = EqyQy("AND", [EqyQy("IS-IN", ["exchange", "NMS", "NYQ"]), EqyQy("LT", ["epsgrowth.lasttwelvemonths", 15])])
day_gainers =  EqyQy("AND", [EqyQy("GT", ["percentchange", 3]), EqyQy("EQ", ["region", "us"]), EqyQy("GTE", ["intradaymarketcap", 2000000000]), EqyQy("GTE", ["intradayprice", 5]), EqyQy("GT", ["dayvolume", 15000])])
day_losers = EqyQy("AND", [EqyQy("LT", ["percentchange", -2.5]), EqyQy("EQ", ["region", "us"]), EqyQy("GTE", ["intradaymarketcap", 2000000000]), EqyQy("GTE", ["intradayprice", 5]), EqyQy("GT", ["dayvolume", 20000])])

# Get from the industry

gold_companies = industry("basic-materials", "gold")
other_companies = industry("basic-materials", ["specialty-chemicals", "building-materials", "copper", "steel"]) 

# Run the query

screen(aggressive_small_caps, sortField="eodvolume", sortAsc=False)
screen(day_gainers, sortField="percentchange", sortAsc=False)
screen(day_losers, sortField="percentchange", sortAsc=True)
screen(gold_companies, sortField="eodvolume", sortAsc=False)
screen(other_companies, sortField="eodvolume", sortAsc=False)