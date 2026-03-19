"""Custom exception hierarchy for yfinance."""


class YFException(Exception):
    """Base exception for yfinance errors."""

    def __init__(self, description=""):
        super().__init__(description)


class YFDataException(YFException):
    """Raised when Yahoo Finance data retrieval fails."""


class YFNotImplementedError(NotImplementedError):
    """Raised when a Yahoo endpoint mapping is not implemented."""

    def __init__(self, method_name):
        super().__init__(f"Have not implemented fetching '{method_name}' from Yahoo API")


class YFTickerMissingError(YFException):
    """Raised when a ticker appears unavailable or delisted."""

    def __init__(self, ticker, rationale):
        super().__init__(f"${ticker}: possibly delisted; {rationale}")
        self.rationale = rationale
        self.ticker = ticker


class YFTzMissingError(YFTickerMissingError):
    """Raised when a ticker timezone cannot be determined."""

    def __init__(self, ticker):
        super().__init__(ticker, "no timezone found")


class YFPricesMissingError(YFTickerMissingError):
    """Raised when Yahoo Finance returns no usable price history."""

    def __init__(self, ticker, debug_info):
        self.debug_info = debug_info
        if debug_info != '':
            super().__init__(ticker, f"no price data found {debug_info}")
        else:
            super().__init__(ticker, "no price data found")


class YFEarningsDateMissing(YFTickerMissingError):
    """Raised when Yahoo Finance has no earnings dates for a ticker."""

    # note that this does not get raised. Added in case of raising it in the future
    def __init__(self, ticker):
        super().__init__(ticker, "no earnings dates found")


class YFInvalidPeriodError(YFException):
    """Raised when a user requests a period unsupported by Yahoo Finance."""

    def __init__(self, ticker, invalid_period, valid_ranges):
        self.ticker = ticker
        self.invalid_period = invalid_period
        self.valid_ranges = valid_ranges
        super().__init__(f"{self.ticker}: Period '{invalid_period}' is invalid, "
                         f"must be one of: {valid_ranges}")


class YFRateLimitError(YFException):
    """Raised when Yahoo Finance rate-limits requests."""

    def __init__(self):
        super().__init__("Too Many Requests. Rate limited. Try after a while.")
