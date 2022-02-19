def get_news_for_tickers(list_tickers):
    list_news = []
    for x in list_tickers:
        news = yf.Ticker(x).news
        list_news.append(x)
        for new in news:
            list_news.append(new['title'] + ' - ' + (new['link']))
    list_news = "[%s]" % ",\n ".join(map(str,list_news))
    return list_news
#  run print(get_news_for_tickers(['MSFT', 'APPLE']))
#  output
#  MSFT,
#  10 Best Stocks to Buy According to Stanley Druckenmiller - https://finance.yahoo.com/news/10-best-stocks-buy-according-184806148.html,
#  Microsoft’s 10 Biggest Acquisitions of All Time - https://finance.yahoo.com/news/microsoft-10-biggest-acquisitions-time-183513066.html,
#  ‘Seismic shift at a glacial pace’ — Microsoft is walking into one of the weirdest times ever for a big merger - https://finance.yahoo.com/m/6609c8b1-326a-3329-b95c-a3fee6d44405/%E2%80%98seismic-shift-at-a-glacial.html,
#  Why Growth and Returns are Surprisingly High for Microsoft (NASDAQ:MSFT) - https://finance.yahoo.com/news/why-growth-returns-surprisingly-high-181810000.html,
#  4 Dow Jones Stocks To Buy And Watch In February 2022: Apple Slides - https://finance.yahoo.com/m/65b53896-faf4-3a06-9d0d-a63cf3c83192/4-dow-jones-stocks-to-buy-and.html,
#  ‘Horizon Forbidden West’ Looms Large in Sony’s PlayStation Strategy - https://finance.yahoo.com/news/horizon-forbidden-west-looms-large-164424663.html,
#  Is Roblox Stock A Buy Right Now After Q4 Earnings Results? - https://finance.yahoo.com/m/08561c49-1aed-38af-b580-d344931bf917/is-roblox-stock-a-buy-right.html,
#  3 Work-From-Home Stocks to Buy Right Now - https://finance.yahoo.com/m/ba0cb351-9680-373e-b604-065fc3625d99/3-work-from-home-stocks-to.html,
#  APPLE,
#  Dow Jones Futures: Biden Says Putin Has Decided To Invade Ukraine; 5 Stocks That Don't Suck - https://finance.yahoo.com/m/88444a8f-9381-3495-abd5-501314c58ba7/dow-jones-futures%3A-biden-says.html,
#  Apple store unions could jeopardize its 'caring' reputation - https://finance.yahoo.com/news/apple-could-jeopardize-its-reputation-if-it-fights-unions-221412117.html,
#  ISS supports Apple shareholder proposal on forced labor - https://finance.yahoo.com/news/iss-supports-apple-shareholder-proposal-213806160.html,
#  Frances Haugen takes us inside Meta HQ - https://finance.yahoo.com/video/frances-haugen-takes-us-inside-110000114.html,
#  Apple is more responsible than Meta, says whistleblower Frances Haugen - https://finance.yahoo.com/news/frances-haugen-apple-more-responsible-than-meta-180524976.html,
#  4 Dow Jones Stocks To Buy And Watch In February 2022: Apple Slides - https://finance.yahoo.com/m/65b53896-faf4-3a06-9d0d-a63cf3c83192/4-dow-jones-stocks-to-buy-and.html,
#  3 Stocks That Could Be Worth More Than Apple by 2035 - https://finance.yahoo.com/m/6446c830-8fdd-3976-a38b-01a798b296ae/3-stocks-that-could-be-worth.html,
#  Frances Haugen explains why Apple is more responsible than Meta - https://finance.yahoo.com/video/frances-haugen-explains-why-apple-153530207.html,
