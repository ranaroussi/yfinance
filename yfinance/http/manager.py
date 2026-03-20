"""Per-call download state manager.

Each yf.download() call constructs its own DownloadManager so that concurrent
calls cannot overwrite each other's results.  This replaces the previous design
where _DFS, _ERRORS, _TRACEBACKS, and _ISINS were process-global dicts in
shared.py that were reset at the start of every download() call.

The fundamental race (issue #2557):
  1. Call A resets shared._DFS to a new empty dict (A_dict).
  2. Call A's workers get a reference to A_dict and begin fetching.
  3. Call B resets shared._DFS to a new empty dict (B_dict), overwriting the
     module attribute.
  4. When Call A's workers execute _get_dfs() they may now get B_dict
     (depending on thread scheduling), and write their results there.
  5. Both callers read from B_dict, so Call A returns Call B's data.

DownloadManager eliminates this by keeping all mutable state on the instance.
No two download() calls share the same manager, so there is nothing to race on.
"""

import threading
import time
from typing import Optional

from .. import utils


class DownloadManager:
    """Encapsulates all mutable state for a single yf.download() call.

    Parameters
    ----------
    tickers : list[str]
        Normalised, upper-cased ticker symbols expected for this call.
    show_progress : bool
        Whether to display an animated progress bar.

    Attributes
    ----------
    dfs : dict[str, DataFrame | None]
        Per-ticker result frames.  Workers always write a slot here, whether
        the fetch succeeded or failed, so the completion check
        ``len(dfs) == len(tickers)`` stays correct.
    errors : dict[str, str]
        Per-ticker error strings for failed downloads.
    tracebacks : dict[str, str]
        Per-ticker traceback strings for failed downloads.
    isins : dict[str, str]
        Resolved ISIN-to-symbol mapping for this call.
    progress_bar : ProgressBar or None
        Animated progress bar, or None when progress=False.
    """

    def __init__(self, tickers: list, show_progress: bool):
        self._tickers = tickers
        self._lock = threading.Lock()
        self.dfs: dict = {}
        self.errors: dict = {}
        self.tracebacks: dict = {}
        self.isins: dict = {}
        self.progress_bar: Optional[utils.ProgressBar] = (
            utils.ProgressBar(len(tickers), "completed") if show_progress else None
        )

    @property
    def tickers(self) -> list:
        """Return the expected ticker symbols for this download call."""
        return self._tickers

    # ------------------------------------------------------------------
    # Result recording — called from worker threads
    # ------------------------------------------------------------------

    def record(
        self,
        symbol: str,
        data,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> None:
        """Store the result for *symbol*.

        Always writes a slot into ``dfs`` so the completion check remains
        correct regardless of whether the fetch succeeded or failed.
        """
        with self._lock:
            self.dfs[symbol] = data
            if error is not None:
                self.errors[symbol] = error
            if traceback_str is not None:
                self.tracebacks[symbol] = traceback_str

    def animate_progress(self) -> None:
        """Advance the progress bar by one tick (called after each ticker completes)."""
        if self.progress_bar is not None:
            self.progress_bar.animate()

    def complete_progress(self) -> None:
        """Mark the progress bar as fully complete."""
        if self.progress_bar is not None:
            self.progress_bar.completed()

    # ------------------------------------------------------------------
    # Completion signalling
    # ------------------------------------------------------------------

    def is_complete(self) -> bool:
        """Return True once every expected ticker has reported a result."""
        with self._lock:
            return len(self.dfs) >= len(self._tickers)

    def wait_for_completion(self) -> None:
        """Block until every ticker has reported a result (success or error)."""
        while not self.is_complete():
            time.sleep(0.01)
