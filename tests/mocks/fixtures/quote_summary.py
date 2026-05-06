"""
Factory for /v10/finance/quoteSummary/{ticker} responses.
"""

import datetime

from ..response import MockResponse
from .chart import _get_meta

# Use a fixed "today" so timestamps are stable during a test run
_NOW_TS = int(datetime.datetime(2024, 11, 1, 12, 0, 0).timestamp())
_QUARTER_DATES = ["2024-09-30", "2024-06-30", "2024-03-31", "2023-12-31"]
_ANNUAL_DATES  = ["2023-12-31", "2022-12-31", "2021-12-31", "2020-12-31"]

# Tickers that return a 404 for quoteSummary.
# Indices (^GSPC, ^DJI, DJI) have chart data but no fundamental summary.
_ERROR_TICKERS = frozenset({"DJI", "^DJI", "^GSPC", "DOES_NOT_EXIST"})


def _raw(value):
    return {"raw": value, "fmt": str(value)}


# ---------------------------------------------------------------------------
# Module builders — each returns a dict for one quoteSummary module
# ---------------------------------------------------------------------------

def _quoteType(ticker, meta):
    exchange, tz_name, currency, instrument_type = meta
    return {
        "maxAge": 1,
        "symbol": ticker,
        "quoteType": instrument_type,
        "shortName": f"{ticker} Inc.",
        "longName": f"{ticker} Incorporated",
        "exchange": exchange,
        "messageBoardId": f"finmb_{ticker.lower()}",
        "exchangeTimezoneName": tz_name,
        "exchangeTimezoneShortName": "EST",
        "isEsgPopulated": False,
        "gmtOffSetMilliseconds": "-18000000",
        "uuid": "00000000-0000-0000-0000-000000000000",
        "market": "us_market",
        "currency": currency,
    }


def _summaryDetail(ticker, meta):
    exchange, tz_name, currency, instrument_type = meta
    return {
        "maxAge": 1,
        "priceHint": _raw(2),
        "previousClose": _raw(150.0),
        "open": _raw(151.0),
        "dayLow": _raw(149.5),
        "dayHigh": _raw(152.0),
        "regularMarketPreviousClose": _raw(150.0),
        "regularMarketOpen": _raw(151.0),
        "regularMarketDayLow": _raw(149.5),
        "regularMarketDayHigh": _raw(152.0),
        "dividendRate": _raw(0.96),
        "dividendYield": _raw(0.0060),
        "exDividendDate": _raw(_NOW_TS - 86400 * 30),
        "payoutRatio": _raw(0.15),
        "fiveYearAvgDividendYield": _raw(0.008),
        "beta": _raw(1.25),
        "trailingPE": _raw(28.5),
        "forwardPE": _raw(25.0),
        "volume": _raw(55_000_000),
        "regularMarketVolume": _raw(55_000_000),
        "averageVolume": _raw(60_000_000),
        "averageVolume10days": _raw(58_000_000),
        "averageDailyVolume10Day": _raw(58_000_000),
        "bid": _raw(150.90),
        "ask": _raw(151.10),
        "bidSize": _raw(8),
        "askSize": _raw(9),
        "marketCap": _raw(2_500_000_000_000),
        "fiftyTwoWeekLow": _raw(125.0),
        "fiftyTwoWeekHigh": _raw(200.0),
        "priceToSalesTrailing12Months": _raw(7.8),
        "fiftyDayAverage": _raw(145.0),
        "twoHundredDayAverage": _raw(140.0),
        "trailingAnnualDividendRate": _raw(0.92),
        "trailingAnnualDividendYield": _raw(0.0061),
        "currency": currency,
        "fromCurrency": None,
        "toCurrency": None,
        "lastMarket": None,
        "coinMarketCapLink": None,
        "algorithm": None,
        "tradeable": False,
    }


def _defaultKeyStatistics(ticker, meta):
    return {
        "maxAge": 1,
        "priceHint": _raw(2),
        "enterpriseValue": _raw(2_600_000_000_000),
        "forwardEps": _raw(6.62),
        "profitMargins": _raw(0.253),
        "floatShares": _raw(15_500_000_000),
        "sharesOutstanding": _raw(15_550_000_000),
        "sharesShort": _raw(98_000_000),
        "sharesShortPriorMonth": _raw(95_000_000),
        "sharesShortPreviousMonthDate": _raw(_NOW_TS - 86400 * 30),
        "dateShortInterest": _raw(_NOW_TS - 86400 * 15),
        "sharesPercentSharesOut": _raw(0.0063),
        "heldPercentInsiders": _raw(0.00076),
        "heldPercentInstitutions": _raw(0.60),
        "shortRatio": _raw(1.64),
        "shortPercentOfFloat": _raw(0.0063),
        "beta": _raw(1.25),
        "impliedSharesOutstanding": _raw(15_550_000_000),
        "bookValue": _raw(4.25),
        "priceToBook": _raw(35.4),
        "lastFiscalYearEnd": _raw(_NOW_TS - 86400 * 30),
        "nextFiscalYearEnd": _raw(_NOW_TS + 86400 * 335),
        "mostRecentQuarter": _raw(_NOW_TS - 86400 * 30),
        "earningsQuarterlyGrowth": _raw(0.30),
        "netIncomeToCommon": _raw(97_000_000_000),
        "trailingEps": _raw(6.13),
        "pegRatio": _raw(2.93),
        "lastSplitFactor": "4:1",
        "lastSplitDate": _raw(1598880000),
        "enterpriseToRevenue": _raw(8.0),
        "enterpriseToEbitda": _raw(22.0),
        "52WeekChange": _raw(0.20),
        "SandP52WeekChange": _raw(0.15),
        "lastDividendValue": _raw(0.24),
        "lastDividendDate": _raw(_NOW_TS - 86400 * 60),
    }


def _assetProfile(ticker, meta):
    return {
        "maxAge": 86400,
        "address1": "One Apple Park Way",
        "city": "Cupertino",
        "state": "CA",
        "zip": "95014",
        "country": "United States",
        "phone": "408 996 1010",
        "website": "https://www.apple.com",
        "industry": "Consumer Electronics",
        "industryKey": "consumer-electronics",
        "industryDisp": "Consumer Electronics",
        "sector": "Technology",
        "sectorKey": "technology",
        "sectorDisp": "Technology",
        "longBusinessSummary": f"{ticker} designs and manufactures consumer electronics and software.",
        "fullTimeEmployees": 161000,
        "companyOfficers": [
            {
                "maxAge": 1,
                "name": "Mr. Timothy D. Cook",
                "age": 62,
                "title": "CEO & Director",
                "yearBorn": 1961,
                "fiscalYear": 2023,
                "totalPay": _raw(63_200_000),
                "exercisedValue": _raw(0),
                "unexercisedValue": _raw(0),
            }
        ],
        "auditRisk": 1,
        "boardRisk": 1,
        "compensationRisk": 3,
        "shareHolderRightsRisk": 1,
        "overallRisk": 1,
        "governanceEpochDate": _raw(_NOW_TS),
        "compensationAsOfEpochDate": _raw(_NOW_TS - 86400 * 180),
        "irWebsite": "http://investor.apple.com/",
    }


def _financialData(ticker, meta):
    return {
        "maxAge": 86400,
        "currentPrice": _raw(150.0),
        "targetHighPrice": _raw(230.0),
        "targetLowPrice": _raw(160.0),
        "targetMeanPrice": _raw(200.0),
        "targetMedianPrice": _raw(200.0),
        "recommendationMean": _raw(1.9),
        "recommendationKey": "buy",
        "numberOfAnalystOpinions": _raw(38),
        "totalCash": _raw(61_555_000_000),
        "totalCashPerShare": _raw(3.97),
        "ebitda": _raw(123_000_000_000),
        "totalDebt": _raw(110_000_000_000),
        "quickRatio": _raw(0.85),
        "currentRatio": _raw(0.99),
        "totalRevenue": _raw(383_285_000_000),
        "debtToEquity": _raw(199.4),
        "revenuePerShare": _raw(24.38),
        "returnOnAssets": _raw(0.203),
        "returnOnEquity": _raw(1.47),
        "grossProfits": _raw(169_148_000_000),
        "freeCashflow": _raw(84_726_000_000),
        "operatingCashflow": _raw(113_260_000_000),
        "earningsGrowth": _raw(0.10),
        "revenueGrowth": _raw(0.01),
        "grossMargins": _raw(0.441),
        "ebitdaMargins": _raw(0.321),
        "operatingMargins": _raw(0.298),
        "profitMargins": _raw(0.253),
        "financialCurrency": "USD",
    }


def _majorHoldersBreakdown(ticker, meta):
    return {
        "maxAge": 1,
        "insidersPercentHeld": 0.00076,
        "institutionsPercentHeld": 0.60,
        "institutionsFloatPercentHeld": 0.60,
        "institutionsCount": 6168,
    }


def _institutionOwnership(ticker, meta):
    return {
        "maxAge": 1,
        "ownershipList": [
            {
                "maxAge": 1,
                "reportDate": {"raw": _NOW_TS - 86400 * 45},
                "organization": "Vanguard Group Inc",
                "position": {"raw": 1_255_000_000},
                "value": {"raw": 218_000_000_000},
                "pctHeld": {"raw": 0.0807},
            },
            {
                "maxAge": 1,
                "reportDate": {"raw": _NOW_TS - 86400 * 45},
                "organization": "Blackrock Inc.",
                "position": {"raw": 1_020_000_000},
                "value": {"raw": 177_000_000_000},
                "pctHeld": {"raw": 0.0657},
            },
        ],
    }


def _fundOwnership(ticker, meta):
    return {
        "maxAge": 1,
        "ownershipList": [
            {
                "maxAge": 1,
                "reportDate": {"raw": _NOW_TS - 86400 * 45},
                "organization": "Vanguard 500 Index Fund",
                "position": {"raw": 330_000_000},
                "value": {"raw": 57_300_000_000},
                "pctHeld": {"raw": 0.0212},
            }
        ],
    }


def _majorDirectHolders(ticker, meta):
    return {
        "maxAge": 1,
        "holders": [
            {
                "maxAge": 1,
                "name": "Timothy D. Cook",
                "relation": "Chief Executive Officer",
                "url": "https://www.sec.gov/cgi-bin/browse-edgar",
                "transactionDescription": "Common Stock",
                "latestTransDate": {"raw": _NOW_TS - 86400 * 60},
                "positionDirect": {"raw": 3_280_275},
                "positionDirectDate": {"raw": _NOW_TS - 86400 * 60},
            }
        ],
    }


def _insiderTransactions(ticker, meta):
    return {
        "maxAge": 1,
        "transactions": [
            {
                "maxAge": 1,
                "shares": {"raw": 100_000},
                "value": {"raw": 15_000_000},
                "filerUrl": "https://www.sec.gov/cgi-bin/browse-edgar",
                "transactionText": "Sale at price $150.00 per share.",
                "filerName": "Cook Timothy D",
                "filerRelation": "Chief Executive Officer",
                "moneyText": "Sale",
                "startDate": {"raw": _NOW_TS - 86400 * 60},
                "ownership": "D",
            }
        ],
    }


def _insiderHolders(ticker, meta):
    return {
        "maxAge": 1,
        "holders": [
            {
                "maxAge": 1,
                "name": "Cook Timothy D",
                "relation": "Chief Executive Officer",
                "url": "https://www.sec.gov/cgi-bin/browse-edgar",
                "transactionDescription": "Sale",
                "latestTransDate": {"raw": _NOW_TS - 86400 * 60},
                "positionDirect": {"raw": 3_280_275},
                "positionDirectDate": {"raw": _NOW_TS - 86400 * 60},
            }
        ],
    }


def _netSharePurchaseActivity(ticker, meta):
    return {
        "maxAge": 1,
        "period": "6m",
        "buyInfoShares": {"raw": 1_000_000},
        "buyInfoAmount": {"raw": 150_000_000},
        "buyPercentInsiderShares": {"raw": 0.001},
        "sellInfoShares": {"raw": 2_000_000},
        "sellInfoAmount": {"raw": 300_000_000},
        "sellPercentInsiderShares": {"raw": 0.002},
        "netInfoShares": {"raw": -1_000_000},
        "netInfoAmount": {"raw": -150_000_000},
        "netPercentInsiderShares": {"raw": -0.001},
    }


def _recommendationTrend(ticker, meta):
    instrument_type = meta[3]
    if instrument_type not in ("EQUITY", "ETF"):
        return {"trend": [], "maxAge": 86400}
    return {
        "trend": [
            {"period": "0m", "strongBuy": 12, "buy": 18, "hold": 7, "sell": 1, "strongSell": 0},
            {"period": "-1m", "strongBuy": 11, "buy": 18, "hold": 8, "sell": 1, "strongSell": 0},
            {"period": "-2m", "strongBuy": 11, "buy": 17, "hold": 8, "sell": 2, "strongSell": 0},
            {"period": "-3m", "strongBuy": 11, "buy": 17, "hold": 9, "sell": 2, "strongSell": 0},
        ],
        "maxAge": 86400,
    }


def _upgradeDowngradeHistory(ticker, meta):
    instrument_type = meta[3]
    if instrument_type not in ("EQUITY", "ETF"):
        return {"history": [], "maxAge": 86400}
    return {
        "history": [
            {
                "epochGradeDate": _NOW_TS - 86400 * 10,
                "firm": "Goldman Sachs",
                "toGrade": "Buy",
                "fromGrade": "Neutral",
                "action": "up",
            },
            {
                "epochGradeDate": _NOW_TS - 86400 * 20,
                "firm": "Morgan Stanley",
                "toGrade": "Overweight",
                "fromGrade": "Equal-Weight",
                "action": "up",
            },
        ],
        "maxAge": 86400,
    }


def _earningsHistory(ticker, meta):
    instrument_type = meta[3]
    if instrument_type not in ("EQUITY", "ETF"):
        return {"maxAge": 1, "history": []}
    return {
        "maxAge": 1,
        "history": [
            {
                "maxAge": 1,
                "epsActual": _raw(1.46),
                "epsEstimate": _raw(1.43),
                "epsDifference": _raw(0.03),
                "surprisePercent": _raw(0.021),
                "quarter": {"raw": _NOW_TS - 86400 * 30, "fmt": _QUARTER_DATES[0]},
                "period": "-3m",
            },
            {
                "maxAge": 1,
                "epsActual": _raw(1.52),
                "epsEstimate": _raw(1.50),
                "epsDifference": _raw(0.02),
                "surprisePercent": _raw(0.013),
                "quarter": {"raw": _NOW_TS - 86400 * 120, "fmt": _QUARTER_DATES[1]},
                "period": "-6m",
            },
        ],
    }


def _earningsTrend(ticker, meta):
    instrument_type = meta[3]
    if instrument_type not in ("EQUITY", "ETF"):
        return {"maxAge": 1, "trend": []}
    return {
        "maxAge": 1,
        "trend": [
            {
                "maxAge": 1,
                "period": "0q",
                "endDate": "2024-12-31",
                "growth": _raw(0.10),
                "earningsEstimate": {
                    "avg": _raw(2.10),
                    "low": _raw(1.95),
                    "high": _raw(2.25),
                    "yearAgoEps": _raw(1.91),
                    "numberOfAnalysts": _raw(28),
                    "growth": _raw(0.10),
                },
                "revenueEstimate": {
                    "avg": _raw(124_000_000_000),
                    "low": _raw(120_000_000_000),
                    "high": _raw(128_000_000_000),
                    "numberOfAnalysts": _raw(25),
                    "yearAgoRevenue": _raw(117_154_000_000),
                    "growth": _raw(0.058),
                },
                "epsTrend": {
                    "current": _raw(2.10),
                    "7daysAgo": _raw(2.08),
                    "30daysAgo": _raw(2.05),
                    "60daysAgo": _raw(2.01),
                    "90daysAgo": _raw(1.99),
                },
                "epsRevisions": {
                    "upLast7days": _raw(3),
                    "upLast30days": _raw(5),
                    "downLast30days": _raw(1),
                    "downLast90days": _raw(2),
                },
            },
            {
                "maxAge": 1,
                "period": "+1q",
                "endDate": "2025-03-31",
                "growth": _raw(0.08),
                "earningsEstimate": {
                    "avg": _raw(1.95),
                    "low": _raw(1.80),
                    "high": _raw(2.10),
                    "yearAgoEps": _raw(1.80),
                    "numberOfAnalysts": _raw(24),
                    "growth": _raw(0.083),
                },
                "revenueEstimate": {
                    "avg": _raw(95_000_000_000),
                    "low": _raw(92_000_000_000),
                    "high": _raw(98_000_000_000),
                    "numberOfAnalysts": _raw(22),
                    "yearAgoRevenue": _raw(90_753_000_000),
                    "growth": _raw(0.047),
                },
                "epsTrend": {
                    "current": _raw(1.95),
                    "7daysAgo": _raw(1.93),
                    "30daysAgo": _raw(1.90),
                    "60daysAgo": _raw(1.88),
                    "90daysAgo": _raw(1.86),
                },
                "epsRevisions": {
                    "upLast7days": _raw(2),
                    "upLast30days": _raw(4),
                    "downLast30days": _raw(1),
                    "downLast90days": _raw(1),
                },
            },
        ],
    }


def _indexTrend(ticker, meta):
    return {
        "maxAge": 1,
        "symbol": "SP5",
        "peRatio": _raw(24.0),
        "pegRatio": _raw(2.5),
        "estimates": [
            {"period": "0q", "growth": _raw(0.073)},
            {"period": "+1q", "growth": _raw(0.082)},
            {"period": "0y", "growth": _raw(0.095)},
            {"period": "+1y", "growth": _raw(0.110)},
        ],
    }


def _sectorTrend(ticker, meta):
    return {
        "maxAge": 1,
        "symbol": None,
        "peRatio": _raw(28.0),
        "pegRatio": _raw(2.9),
        "estimates": [],
    }


def _calendarEvents(ticker, meta):
    return {
        "maxAge": 1,
        "earnings": {
            "earningsDate": [_NOW_TS + 86400 * 30, _NOW_TS + 86400 * 38],
            "earningsCallDate": [],
            "isEarningsDateEstimate": True,
            "earningsAverage": _raw(2.10),
            "earningsLow": _raw(1.95),
            "earningsHigh": _raw(2.25),
            "revenueAverage": _raw(124_000_000_000),
            "revenueLow": _raw(120_000_000_000),
            "revenueHigh": _raw(128_000_000_000),
        },
        "exDividendDate": _NOW_TS - 86400 * 30,
        "dividendDate": _NOW_TS - 86400 * 15,
    }


def _esgScores(ticker, meta):
    return {
        "maxAge": 86400,
        "totalEsg": _raw(15.0),
        "environmentScore": _raw(3.5),
        "socialScore": _raw(6.0),
        "governanceScore": _raw(5.5),
        "ratingYear": 2023,
        "ratingMonth": 10,
        "highestControversy": 0,
        "percentile": _raw(72.0),
        "esgPerformance": "UNDER_PERF",
        "peerGroup": "Software—Application",
        "peerCount": 56,
        "relatedControversy": [],
        "peerEsgScorePerformance": {"min": _raw(5.0), "avg": _raw(16.2), "max": _raw(35.0)},
        "peerGovernancePerformance": {"min": _raw(1.0), "avg": _raw(5.8), "max": _raw(15.0)},
        "peerSocialPerformance": {"min": _raw(2.0), "avg": _raw(6.8), "max": _raw(20.0)},
        "peerEnvironmentPerformance": {"min": _raw(0.5), "avg": _raw(3.5), "max": _raw(12.0)},
        "peerHighestControversyPerformance": {"min": 0, "avg": 1.5, "max": 4},
        "adult": False,
        "alcoholic": False,
        "animalTesting": False,
        "catholic": False,
        "controversialWeapons": False,
        "smallArms": False,
        "furLeather": False,
        "gambling": False,
        "gmo": False,
        "militaryContract": False,
        "nuclear": False,
        "pesticides": False,
        "palmOil": False,
        "coal": False,
        "tobacco": False,
    }


def _summaryProfile(ticker, meta):
    exchange, tz_name, currency, instrument_type = meta
    if instrument_type not in ("ETF", "MUTUALFUND"):
        return None
    return {
        "maxAge": 86400,
        "longBusinessSummary": f"{ticker} is a fund that seeks to track a benchmark index.",
        "address1": "100 Vanguard Blvd.",
        "city": "Malvern",
        "state": "PA",
        "country": "United States",
        "phone": "800-662-7447",
        "website": "https://www.vanguard.com",
        "industry": "",
        "sector": "",
    }


def _topHoldings(ticker, meta):
    exchange, tz_name, currency, instrument_type = meta
    if instrument_type not in ("ETF", "MUTUALFUND"):
        return None
    return {
        "maxAge": 1,
        "cashPosition": {"raw": 0.02},
        "stockPosition": {"raw": 0.97},
        "bondPosition": {"raw": 0.01},
        "preferredPosition": {"raw": 0.0},
        "convertiblePosition": {"raw": 0.0},
        "otherPosition": {"raw": 0.0},
        "holdings": [
            {"symbol": "AAPL",  "holdingName": "Apple Inc.",   "holdingPercent": 0.07},
            {"symbol": "MSFT",  "holdingName": "Microsoft",    "holdingPercent": 0.065},
            {"symbol": "GOOGL", "holdingName": "Alphabet Inc.","holdingPercent": 0.04},
        ],
        "equityHoldings": {
            "priceToEarnings":    {"raw": 26.5},
            "priceToBook":        {"raw": 4.8},
            "priceToSales":       {"raw": 2.9},
            "priceToCashflow":    {"raw": 20.1},
            "medianMarketCap":    {"raw": 112_000_000_000},
            "threeYearEarningsGrowth": {"raw": 0.15},
            "priceToEarningsCat":{"raw": 25.0},
            "priceToBookCat":     {"raw": 4.5},
            "priceToSalesCat":    {"raw": 2.8},
            "priceToCashflowCat": {"raw": 19.5},
            "medianMarketCapCat": {"raw": 100_000_000_000},
            "threeYearEarningsGrowthCat": {"raw": 0.12},
        },
        "bondHoldings": {},
        "bondRatings": [],
        "sectorWeightings": [
            {"Technology": 0.30},
            {"Healthcare":  0.12},
            {"Financials":  0.13},
        ],
    }


def _fundProfile(ticker, meta):
    exchange, tz_name, currency, instrument_type = meta
    if instrument_type not in ("ETF", "MUTUALFUND"):
        return None
    return {
        "maxAge": 86400,
        "categoryName": "Large Blend",
        "family": "Vanguard",
        "legalType": "Exchange Traded Fund",
        "feesExpensesInvestment": {
            "annualReportExpenseRatio": {"raw": 0.0003},
            "annualHoldingsTurnover":   {"raw": 0.04},
            "totalNetAssets":           {"raw": 400_000_000_000},
        },
        "feesExpensesInvestmentCat": {
            "annualReportExpenseRatio": {"raw": 0.0050},
            "annualHoldingsTurnover":   {"raw": 0.25},
            "totalNetAssets":           {"raw": 10_000_000_000},
        },
    }


def _secFilings(ticker, meta):
    return {
        "filings": [
            {
                "date": "2024-11-01",
                "epochDate": _NOW_TS,
                "type": "10-K",
                "title": "Annual Report",
                "edgarUrl": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}",
                "maxAge": 86400,
            }
        ],
        "maxAge": 86400,
    }


# ---------------------------------------------------------------------------
# Module dispatch table
# ---------------------------------------------------------------------------

_MODULE_BUILDERS = {
    "quoteType":                _quoteType,
    "summaryDetail":            _summaryDetail,
    "defaultKeyStatistics":     _defaultKeyStatistics,
    "assetProfile":             _assetProfile,
    "summaryProfile":           _summaryProfile,
    "financialData":            _financialData,
    "majorHoldersBreakdown":    _majorHoldersBreakdown,
    "institutionOwnership":     _institutionOwnership,
    "fundOwnership":            _fundOwnership,
    "majorDirectHolders":       _majorDirectHolders,
    "insiderTransactions":      _insiderTransactions,
    "insiderHolders":           _insiderHolders,
    "netSharePurchaseActivity": _netSharePurchaseActivity,
    "recommendationTrend":      _recommendationTrend,
    "upgradeDowngradeHistory":  _upgradeDowngradeHistory,
    "earningsHistory":          _earningsHistory,
    "earningsTrend":            _earningsTrend,
    "indexTrend":               _indexTrend,
    "sectorTrend":              _sectorTrend,
    "calendarEvents":           _calendarEvents,
    "esgScores":                _esgScores,
    "secFilings":               _secFilings,
    "topHoldings":              _topHoldings,
    "fundProfile":              _fundProfile,
}


def make_response(ticker, params):
    params = params or {}

    if ticker.upper() in _ERROR_TICKERS:
        return MockResponse(status_code=404)

    modules_str = params.get("modules", "")
    requested = [m.strip() for m in modules_str.split(",") if m.strip()]

    meta = _get_meta(ticker)
    result = {}
    for mod in requested:
        builder = _MODULE_BUILDERS.get(mod)
        if builder:
            built = builder(ticker, meta)
            if built is not None:
                result[mod] = built
        # Unknown modules: silently skip (caller does .get(module, {}))

    return MockResponse({
        "quoteSummary": {
            "result": [result],
            "error": None,
        }
    })


def make_v7_quote_response(ticker, params):
    """For /v7/finance/quote? — the additional info endpoint."""
    exchange, tz_name, currency, instrument_type = _get_meta(ticker)
    return MockResponse({
        "quoteResponse": {
            "result": [{
                "symbol": ticker,
                "language": "en-US",
                "region": "US",
                "quoteType": instrument_type,
                "typeDisp": instrument_type.capitalize(),
                "quoteSourceName": "Delayed Quote",
                "triggerable": True,
                "customPriceAlertConfidence": "HIGH",
                "currency": currency,
                "exchange": exchange,
                "shortName": f"{ticker} Inc.",
                "longName": f"{ticker} Incorporated",
                "messageBoardId": f"finmb_{ticker.lower()}",
                "exchangeTimezoneName": tz_name,
                "exchangeTimezoneShortName": "EST",
                "gmtOffSetMilliseconds": -18000000,
                "market": "us_market",
                "esgPopulated": False,
                "regularMarketPrice": 150.0,
                "regularMarketTime": _NOW_TS,
                "regularMarketChange": 1.5,
                "regularMarketOpen": 151.0,
                "regularMarketDayHigh": 152.0,
                "regularMarketDayLow": 149.5,
                "regularMarketVolume": 55_000_000,
                "regularMarketPreviousClose": 148.5,
                "bid": 150.9,
                "ask": 151.1,
                "bidSize": 8,
                "askSize": 9,
                "fullExchangeName": exchange,
                "financialCurrency": currency,
                "averageDailyVolume3Month": 60_000_000,
                "averageDailyVolume10Day": 58_000_000,
                "fiftyTwoWeekLowChange": 25.0,
                "fiftyTwoWeekLowChangePercent": 0.20,
                "fiftyTwoWeekRange": "125.0 - 200.0",
                "fiftyTwoWeekHighChange": -50.0,
                "fiftyTwoWeekHighChangePercent": -0.25,
                "fiftyTwoWeekLow": 125.0,
                "fiftyTwoWeekHigh": 200.0,
                "fiftyTwoWeekChangePercent": 0.20,
                "fiftyDayAverage": 145.0,
                "twoHundredDayAverage": 140.0,
                "fiftyDayAverageChange": 5.0,
                "fiftyDayAverageChangePercent": 0.034,
                "twoHundredDayAverageChange": 10.0,
                "twoHundredDayAverageChangePercent": 0.071,
                "marketCap": 2_500_000_000_000,
                "forwardPE": 25.0,
                "priceToBook": 35.4,
                "sourceInterval": 15,
                "exchangeDataDelayedBy": 0,
                "firstTradeDateMilliseconds": 345479400000,
                "priceHint": 2,
                "isin": f"US{ticker[:5].upper()}000000",
                "tradeable": False,
                "cryptoTradeable": False,
            }],
            "error": None,
        }
    })
