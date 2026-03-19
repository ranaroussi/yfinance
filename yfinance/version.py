"""Package version metadata."""

VERSION = "2.0.0"


def __getattr__(name):
    if name == "version":
        return VERSION
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
