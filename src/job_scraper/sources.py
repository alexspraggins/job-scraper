"""Job-board source adapters and bounded source execution."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import multiprocessing
from queue import Empty
import time
from typing import Callable

import pandas as pd
from jobspy import scrape_jobs

from . import config


JOBSPY_SOURCES = frozenset({"indeed", "linkedin", "glassdoor"})


@dataclass(frozen=True)
class SourceRequest:
    """One source/search request made by the scrape pipeline."""

    source: str
    search_term: str
    lookback_hours: int


@dataclass(frozen=True)
class SourceResult:
    """Normalized outcome of one source/search request."""

    request: SourceRequest
    jobs: pd.DataFrame
    duration_ms: int
    error: str | None = None

    @property
    def result_count(self) -> int:
        return len(self.jobs)


class _ErrorCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message and message not in self.messages:
            self.messages.append(message)


def validate_sources(sources: list[str]) -> None:
    unknown = sorted(set(sources) - JOBSPY_SOURCES)
    if unknown:
        supported = ", ".join(sorted(JOBSPY_SOURCES))
        raise ValueError(
            f"Unsupported source(s): {', '.join(unknown)}. "
            f"Supported sources: {supported}"
        )


def scrape_source(
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame] = scrape_jobs,
) -> pd.DataFrame:
    """Fetch one source using the common JobSpy input contract."""
    return scraper(
        site_name=[source],
        search_term=search_term,
        location=config.LOCATION,
        results_wanted=config.RESULTS_PER_QUERY,
        hours_old=lookback_hours,
        country_indeed=config.COUNTRY,
    )


def _scrape_source_with_errors(
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    capture = _ErrorCapture()
    root_logger = logging.getLogger()
    root_logger.addHandler(capture)
    try:
        jobs = scrape_source(source, search_term, lookback_hours, scraper)
    finally:
        root_logger.removeHandler(capture)
    return jobs, tuple(capture.messages)


def _scrape_worker(
    result_queue,
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
) -> None:
    try:
        jobs, errors = _scrape_source_with_errors(
            source, search_term, lookback_hours, scraper
        )
        result_queue.put(
            ("ok", (jobs, errors))
        )
    except Exception as error:
        result_queue.put(("error", f"{type(error).__name__}: {error}"))


def _scrape_source_with_timeout_result(
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
    timeout_seconds: float,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """Fetch one source in a process so a stalled request can be terminated."""
    if timeout_seconds <= 0:
        return _scrape_source_with_errors(source, search_term, lookback_hours, scraper)

    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue(maxsize=1)
    process = context.Process(
        target=_scrape_worker,
        args=(result_queue, source, search_term, lookback_hours, scraper),
    )
    process.start()
    try:
        result_type, payload = result_queue.get(timeout=timeout_seconds)
    except Empty as error:
        if process.is_alive():
            process.terminate()
            process.join(5)
            raise TimeoutError(
                f"source request exceeded {timeout_seconds:g} seconds"
            ) from error
        raise RuntimeError(
            f"source worker exited without a result (exit code {process.exitcode})"
        ) from error
    finally:
        result_queue.close()

    process.join(5)
    if process.is_alive():
        process.terminate()
        process.join(5)

    if result_type == "error":
        raise RuntimeError(payload)
    if isinstance(payload, tuple) and len(payload) == 2:
        return payload
    return payload, ()


def scrape_source_with_timeout(
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
    timeout_seconds: float,
) -> pd.DataFrame:
    """Compatibility wrapper returning only the fetched job frame."""
    jobs, _ = _scrape_source_with_timeout_result(
        source, search_term, lookback_hours, scraper, timeout_seconds
    )
    return jobs


def run_source_attempt(
    request: SourceRequest,
    *,
    scraper: Callable[..., pd.DataFrame],
    timeout_seconds: float,
) -> SourceResult:
    """Run a source request and convert failures into typed results."""
    started = time.perf_counter()
    try:
        jobs, logged_errors = _scrape_source_with_timeout_result(
            request.source,
            request.search_term,
            request.lookback_hours,
            scraper,
            timeout_seconds,
        )
        if jobs is None:
            jobs = pd.DataFrame()
        return SourceResult(
            request=request,
            jobs=jobs,
            duration_ms=round((time.perf_counter() - started) * 1000),
            error="; ".join(logged_errors) if logged_errors else None,
        )
    except Exception as error:
        return SourceResult(
            request=request,
            jobs=pd.DataFrame(),
            duration_ms=round((time.perf_counter() - started) * 1000),
            error=f"{type(error).__name__}: {error}",
        )
