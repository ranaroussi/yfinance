class YFException(Exception):
    """Custom exception for handling specific errors in the application.

    This exception is used to signal errors related to specific operations 
    or conditions within the application that require a custom error message.

    Args:
        description (str, optional): A human-readable description of the error. 
            Defaults to an empty string."""
    def __init__(self, description=""):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        super().__init__(description)


class YFDataException(YFException):
    """Exception raised for errors related to Yahoo Finance data operations.

    This class is a specific type of YFException, used to indicate issues 
    encountered while handling data from Yahoo Finance.

    Attributes:
        None"""
    pass


class YFNotImplementedError(NotImplementedError):
    """Exception raised for unimplemented methods in Yahoo API fetching.

    This exception is used to indicate that a specific method for fetching
    data from the Yahoo API has not been implemented.

    Args:
        method_name (str): The name of the method that is not implemented."""
    def __init__(self, method_name):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        super().__init__(f"Have not implemented fetching '{method_name}' from Yahoo API")


class YFTickerMissingError(YFException):
    def __init__(self, ticker, rationale):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        super().__init__(f"${ticker}: possibly delisted; {rationale}")
        self.rationale = rationale
        self.ticker = ticker


class YFTzMissingError(YFTickerMissingError):
    def __init__(self, ticker):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        super().__init__(ticker, "no timezone found")


class YFPricesMissingError(YFTickerMissingError):
    def __init__(self, ticker, debug_info):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        self.debug_info = debug_info
        if debug_info != '':
            super().__init__(ticker, f"no price data found {debug_info}")
        else:
            super().__init__(ticker, "no price data found")


class YFEarningsDateMissing(YFTickerMissingError):
    # note that this does not get raised. Added in case of raising it in the future
    def __init__(self, ticker):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        super().__init__(ticker, "no earnings dates found")


class YFInvalidPeriodError(YFException):
    def __init__(self, ticker, invalid_period, valid_ranges):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        self.ticker = ticker
        self.invalid_period = invalid_period
        self.valid_ranges = valid_ranges
        super().__init__(f"{self.ticker}: Period '{invalid_period}' is invalid, "
                         f"must be of the format {valid_ranges}, etc.")


class YFRateLimitError(YFException):
    def __init__(self):
        """Initializes an exception with a message indicating rate limiting.

    This exception is raised when too many requests have been made in a short period,
    prompting the user to try again after some time.

    Args:
        None

    Returns:
        None"""
        super().__init__("Too Many Requests. Rate limited. Try after a while.")
