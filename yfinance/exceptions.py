class YFException(Exception):
    def __init__(self, description=""):
        super().__init__(description)


class YFDataException(YFException):
    pass


class YFChartError(YFException):
    def __init__(self, ticker, description):
        self.ticker = ticker
        super().__init__(f"{self.ticker}: {description}")


class YFNotImplementedError(NotImplementedError):
    def __init__(self, method_name):
        super().__init__(f"Have not implemented fetching '{method_name}' from Yahoo API")


class YFTickerMissingError(YFException):
    def __init__(self, ticker, rationale):
        super().__init__(f"${ticker}: possibly delisted; {rationale}")
        self.rationale = rationale
        self.ticker = ticker


class YFTzMissingError(YFTickerMissingError):
    def __init__(self, ticker):
        super().__init__(ticker, "No timezone found")


class YFPricesMissingError(YFTickerMissingError):
    def __init__(self, ticker, debug_info):
        self.debug_info = debug_info
        super().__init__(ticker, f"No price data found {debug_info}")


class YFEarningsDateMissing(YFTickerMissingError):
    # note that this does not get raised. Added in case of raising it in the future
    def __init__(self, ticker):
        super().__init__(ticker, "No earnings dates found")


class YFInvalidPeriodError(YFException):
    def __init__(self, ticker, invalid_period, valid_ranges):
        self.ticker = ticker
        self.invalid_period = invalid_period
        self.valid_ranges = valid_ranges
        super().__init__(f"{self.ticker}: Period '{invalid_period}' is invalid, must be one of {valid_ranges}")
