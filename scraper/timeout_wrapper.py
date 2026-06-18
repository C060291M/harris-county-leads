"""
timeout_wrapper.py — hard time budget for Tyler iDS scraper functions

Drop this into any multi_county_*.py scraper to enforce a wall-clock limit.
When the budget expires the scraper saves whatever records it already collected
and exits cleanly (exit code 0) instead of being killed by GitHub Actions
(which produces NO output file and therefore 0 leads in the DB).

Usage in a scraper:
    from scraper_config import get_county_timeout
    from timeout_wrapper import run_with_timeout

    records = []
    success = run_with_timeout(
        scrape_function,          # the async def that fills `records`
        args=(page, county, ...),
        timeout_seconds=get_county_timeout(county),
        partial_results=records,  # list that gets filled as scraping runs
        county=county,
    )
    # records now contains whatever was scraped before timeout

The key design: the scraper fills a shared list as it goes (append each
record after parsing). timeout_wrapper cancels the task when time is up
and the list already has the partial results — nothing is lost.
"""

import asyncio
import logging
import signal
import time
from typing import Any, Callable, Coroutine

log = logging.getLogger("timeout_wrapper")


async def run_with_timeout(
    coro_func: Callable[..., Coroutine],
    args: tuple = (),
    kwargs: dict = None,
    timeout_seconds: int = 4800,
    county: str = "unknown",
) -> tuple[bool, Any]:
    """
    Run an async coroutine with a hard timeout.
    
    Returns (completed_normally: bool, result: Any)
    
    If timeout fires: returns (False, None) — caller should use partial_results list
    If completed:     returns (True, return_value)
    """
    kwargs = kwargs or {}
    start = time.monotonic()

    try:
        result = await asyncio.wait_for(
            coro_func(*args, **kwargs),
            timeout=timeout_seconds,
        )
        elapsed = time.monotonic() - start
        log.info(f"[{county}] Completed in {elapsed:.0f}s")
        return True, result

    except asyncio.TimeoutError:
        elapsed = time.monotonic() - start
        log.warning(
            f"[{county}] Hard timeout after {elapsed:.0f}s "
            f"(limit={timeout_seconds}s) — saving partial results"
        )
        return False, None

    except Exception as e:
        elapsed = time.monotonic() - start
        log.error(f"[{county}] Error after {elapsed:.0f}s: {e}")
        raise


class TimeboxedScraper:
    """
    Context manager that enforces a wall-clock budget across multiple operations.
    
    Use this inside a scraper loop to check remaining time after each page:
    
        budget = TimeboxedScraper(timeout_seconds=3600, county=county)
        async with budget:
            for page_num in range(max_pages):
                if budget.expired:
                    log.warning(f"[{county}] Budget expired at page {page_num}, stopping")
                    break
                records = await scrape_page(page, page_num)
                all_records.extend(records)
                budget.checkpoint(f"page {page_num}: {len(records)} records")
    """

    def __init__(self, timeout_seconds: int, county: str = "unknown"):
        self.timeout_seconds = timeout_seconds
        self.county = county
        self._start: float = 0.0
        self._checkpoints: list[str] = []

    async def __aenter__(self):
        self._start = time.monotonic()
        log.info(f"[{self.county}] Budget started: {self.timeout_seconds}s")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        elapsed = time.monotonic() - self._start
        log.info(f"[{self.county}] Budget used: {elapsed:.0f}s / {self.timeout_seconds}s")
        return False  # don't suppress exceptions

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start

    @property
    def remaining(self) -> float:
        return max(0.0, self.timeout_seconds - self.elapsed)

    @property
    def expired(self) -> bool:
        return self.elapsed >= self.timeout_seconds

    def checkpoint(self, label: str = ""):
        msg = f"[{self.county}] {self.elapsed:.0f}s elapsed | {self.remaining:.0f}s left"
        if label:
            msg += f" | {label}"
        log.debug(msg)
        self._checkpoints.append(msg)

    def should_start_next_page(self, avg_page_seconds: float = 300) -> bool:
        """
        Returns False if there isn't enough time left to complete another page.
        avg_page_seconds: rolling average time per page (default 5 min conservative est.)
        """
        # Need at least 1.5x avg page time + 2 min buffer to save results
        needed = (avg_page_seconds * 1.5) + 120
        ok = self.remaining >= needed
        if not ok:
            log.warning(
                f"[{self.county}] Skipping next page — "
                f"only {self.remaining:.0f}s left, need ~{needed:.0f}s"
            )
        return ok
