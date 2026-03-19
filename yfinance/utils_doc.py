"""Docstring and CLI formatting helpers used by yfinance."""

import re as _re
import sys as _sys
import textwrap
from typing import Optional, Sequence


def dynamic_docstring(placeholders: dict):
    """Replace placeholders in function docstrings using the provided mapping."""

    def decorator(func):
        if func.__doc__:
            docstring = func.__doc__
            for key, value in placeholders.items():
                docstring = docstring.replace(f"{{{key}}}", value)
            func.__doc__ = docstring
        return func

    return decorator


def _generate_table_configurations(title: Optional[str] = None) -> str:
    """Return the common reStructuredText list-table header."""
    table_title = title or "Permitted Keys/Values"
    return textwrap.dedent(
        f"""
        .. list-table:: {table_title}
           :widths: 25 75
           :header-rows: 1

           * - Key
             - Values
        """
    )


def generate_list_table_from_dict(
    data: dict,
    bullets: bool = True,
    title: Optional[str] = None,
) -> str:
    """Generate a list-table string from a dictionary of iterables."""
    table = _generate_table_configurations(title)
    for key in sorted(data.keys()):
        values = data[key]
        table += " " * 3 + f"* - {key}\n"
        lengths = [len(str(v)) for v in values]
        if bullets and max(lengths) > 5:
            table += " " * 5 + "-\n"
            for value in sorted(values):
                table += " " * 7 + f"- {value}\n"
            continue
        value_str = ", ".join(sorted(values))
        table += " " * 5 + f"- {value_str}\n"
    return table


def _normalize_nested_values(value):
    """Normalize nested list/set/dict values before rendering."""
    if isinstance(value, set):
        return sorted(list(value))
    if isinstance(value, dict) and len(value) == 0:
        return []
    if isinstance(value, list):
        values = sorted(value)
        all_scalar = all(isinstance(item, (int, float, str)) for item in values)
        if all_scalar:
            return _re.sub(r"[{}\[\]']", "", str(values))
        return values
    return value


def _append_scalar_or_block(
    table_add: str,
    nested_key: str,
    nested_value_str: str,
    index: int,
    block_format: bool,
) -> str:
    """Append one nested key/value item to the table output."""
    table_add += " " * 5
    table_add += "- " if index == 0 else "  "

    if "\n" in nested_value_str:
        table_add += "| " + f"{nested_key}: " + "\n"
        for line_index, line in enumerate(nested_value_str.split("\n")):
            table_add += " " * 7 + "|" + " " * 5 + line
            if line_index < len(nested_value_str.split("\n")) - 1:
                table_add += "\n"
        return table_add + "\n"

    table_add += "| " if block_format else "* "
    table_add += f"{nested_key}: " + nested_value_str + "\n"
    return table_add


def _render_nested_dict_values(
    values: dict,
    concat_short_lines: bool,
    bullets: bool,
) -> str:
    """Render nested dictionary values into list-table rows."""
    table_add = ""
    if not bullets:
        return table_add + " " * 5 + f"- {values}\n"

    nested_keys = sorted(list(values.keys()))
    current_line = ""
    block_format = "query" in nested_keys

    for index, nested_key in enumerate(nested_keys):
        nested_value = _normalize_nested_values(values[nested_key])
        nested_value_str = (
            nested_value if isinstance(nested_value, str) else str(nested_value)
        )

        if len(current_line) > 0 and len(current_line) + len(nested_value_str) > 40:
            table_add += current_line + "\n"
            current_line = ""

        if concat_short_lines:
            if current_line == "":
                current_line += " " * 5
                current_line += "- " if index == 0 else "  "
                current_line += "| "
            else:
                current_line += ".  "
            current_line += f"{nested_key}: " + nested_value_str
            continue

        table_add = _append_scalar_or_block(
            table_add,
            nested_key,
            nested_value_str,
            index,
            block_format,
        )

    if current_line != "":
        table_add += current_line + "\n"
    return table_add


def generate_list_table_from_dict_universal(
    data: dict,
    bullets: bool = True,
    title: Optional[str] = None,
    concat_keys: Optional[Sequence[str]] = None,
) -> str:
    """Generate list-table text from dictionaries with scalar or nested values."""
    concat_keys_set = set(concat_keys or [])
    table = _generate_table_configurations(title)

    for key, values in data.items():
        table += " " * 3 + f"* - {key}\n"
        if isinstance(values, dict):
            table += _render_nested_dict_values(
                values,
                concat_short_lines=key in concat_keys_set,
                bullets=bullets,
            )
            continue

        lengths = [len(str(v)) for v in values]
        if bullets and max(lengths) > 5:
            table += " " * 5 + "-\n"
            for value in sorted(values):
                table += " " * 7 + f"- {value}\n"
            continue
        value_str = ", ".join(sorted(values))
        table += " " * 5 + f"- {value_str}\n"

    return table


class ProgressBar:
    """Simple terminal progress bar for long-running operations."""

    def __init__(self, iterations, text="completed"):
        self.text = text
        self.iterations = iterations
        self.prog_bar = "[]"
        self.fill_char = "*"
        self.width = 50
        self.__update_amount(0)
        self.elapsed = 1

    def completed(self):
        """Mark progress complete and render a final newline."""
        self.elapsed = min(self.elapsed, self.iterations)
        self.update_iteration(1)
        print("\r" + str(self), end="", file=_sys.stderr)
        _sys.stderr.flush()
        print("", file=_sys.stderr)

    def animate(self, iteration=None):
        """Advance progress by one step or a provided increment."""
        if iteration is None:
            self.elapsed += 1
            iteration = self.elapsed
        else:
            self.elapsed += iteration

        print("\r" + str(self), end="", file=_sys.stderr)
        _sys.stderr.flush()
        self.update_iteration()

    def update_iteration(self, val=None):
        """Update rendered progress to current state."""
        value = val if val is not None else self.elapsed / float(self.iterations)
        self.__update_amount(value * 100.0)
        self.prog_bar += f"  {self.elapsed} of {self.iterations} {self.text}"

    def __update_amount(self, new_amount):
        percent_done = int(round((new_amount / 100.0) * 100.0))
        all_full = self.width - 2
        num_hashes = int(round((percent_done / 100.0) * all_full))
        self.prog_bar = (
            "[" + self.fill_char * num_hashes + " " * (all_full - num_hashes) + "]"
        )
        pct_place = (len(self.prog_bar) // 2) - len(str(percent_done))
        pct_string = f"{percent_done}%"
        self.prog_bar = self.prog_bar[0:pct_place] + (
            pct_string + self.prog_bar[pct_place + len(pct_string) :]
        )

    def __str__(self):
        """Return progress bar text for printing."""
        return str(self.prog_bar)
