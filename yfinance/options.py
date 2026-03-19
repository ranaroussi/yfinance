"""Internal option parsing helpers shared by history and download APIs."""

from typing import Any, Mapping, Sequence


HISTORY_REQUEST_ARG_NAMES = (
    "period",
    "interval",
    "start",
    "end",
    "prepost",
    "actions",
    "auto_adjust",
    "back_adjust",
    "repair",
    "keepna",
    "rounding",
    "timeout",
)
HISTORY_REQUEST_DEFAULTS = {
    "period": None,
    "interval": "1d",
    "start": None,
    "end": None,
    "prepost": False,
    "actions": True,
    "auto_adjust": True,
    "back_adjust": False,
    "repair": False,
    "keepna": False,
    "rounding": False,
    "timeout": 10,
}

TICKERS_DOWNLOAD_ARG_NAMES = (
    "period",
    "interval",
    "start",
    "end",
    "prepost",
    "actions",
    "auto_adjust",
    "repair",
    "threads",
    "group_by",
    "progress",
    "timeout",
)
TICKERS_DOWNLOAD_DEFAULTS = {
    "period": None,
    "interval": "1d",
    "start": None,
    "end": None,
    "prepost": False,
    "actions": True,
    "auto_adjust": True,
    "repair": False,
    "threads": True,
    "group_by": "column",
    "progress": True,
    "timeout": 10,
}


def bind_options(
    function_name: str,
    arg_names: Sequence[str],
    defaults: Mapping[str, Any],
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
):
    """Bind positional and keyword arguments onto an option dictionary."""
    if len(args) > len(arg_names):
        raise TypeError(
            f"{function_name}() takes at most {len(arg_names)} positional arguments "
            f"({len(args)} given)"
        )

    options = dict(defaults)
    remaining_kwargs = dict(kwargs)

    for key, value in zip(arg_names, args):
        if key in remaining_kwargs:
            raise TypeError(f"{function_name}() got multiple values for argument '{key}'")
        options[key] = value

    for key in arg_names:
        if key in remaining_kwargs:
            options[key] = remaining_kwargs.pop(key)

    return options, remaining_kwargs
