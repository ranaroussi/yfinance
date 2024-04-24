class YFinanceException(Exception):
    def __init__(self, description=""):
        super().__init__(description)


class YFinanceDataException(YFinanceException):
    pass


class YFinanceChartError(YFinanceException):
    def __init__(self, ticker, description):
        self.ticker = ticker
        super().__init__(f"{self.ticker}: {description}")


class YFNotImplementedError(NotImplementedError):
    def __init__(self, method_name):
        super().__init__(f"Have not implemented fetching '{method_name}' from Yahoo API")


class YFinanceTickerMissingError(YFinanceException):
    def __init__(self, ticker, rationale):
        super().__init__(f"${ticker}: possibly delisted; {rationale}")
        self.rationale = rationale
        self.ticker = rationale


class YFinanceTimezoneMissingError(YFinanceTickerMissingError):
    def __init__(self, ticker):
        super().__init__(ticker, "No timezone found")


class YFinancePriceDataMissingError(YFinanceTickerMissingError):
    def __init__(self, ticker, debug_info):
        self.debug_info = debug_info
        super().__init__(ticker, f"No price data found {debug_info}")


class YFinanceEarningsDateMissing(YFinanceTickerMissingError):
    def __init__(self, ticker):
        super().__init__(ticker, "No earnings dates found")


class YFinanceInvalidPeriodError(YFinanceException):
    def __init__(self, ticker, invalid_period, valid_ranges):
        self.ticker = ticker
        self.invalid_period = invalid_period
        self.valid_ranges = valid_ranges
        super().__init__(f"{self.ticker}: Period '{invalid_period}' is invalid, must be one of {valid_ranges}")
