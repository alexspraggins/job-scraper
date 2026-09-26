"""Fetch, filter, persist, enrich, and export one scrape cycle."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
import sys
import time
from typing import Callable

import pandas as pd

from . import config
from .enrichment import EnrichmentStats, process_enrichment
from .filtering import filter_jobs
from .output import export_all
from .queueing import EnrichmentQueue
from .sources import (
    SourceRequest,
    SourceResult,
    run_source_attempt,
    scrape_jobs as default_scraper,
    validate_sources,
)
from .storage import JobStore


LOGGER = logging.getLogger("job_scraper")


def _configure_verbose_logging(enabled: bool) -> None:
    if not enabled:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOGGER.setLevel(logging.INFO)


@dataclass(frozen=True)
class RunSummary:
    run_id: int
    raw_count: int
    accepted_count: int
    excluded_count: int
    unmatched_count: int
    duplicate_count: int
    new_job_ids: tuple[int, ...]
    errors: tuple[str, ...]
    current_export: Path
    new_export: Path | None
    enrichment: EnrichmentStats
    fetch_tasks_created: int = 0
    analysis_tasks_created: int = 0


def run_scrape_cycle(
    store: JobStore,
    lookback_hours: int,
    *,
    search_groups: dict[str, list[str]] | None = None,
    sources: list[str] | None = None,
    scraper: Callable[..., pd.DataFrame] | None = None,
    query_delay_seconds: float | None = None,
    source_timeout_seconds: float | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    export_dir: str | Path | None = None,
    process_queues: bool = True,
    verbose_logging: bool | None = None,
    enrichment_processor: Callable[..., EnrichmentStats] | None = None,
    exporter: Callable[..., tuple[Path, Path | None]] | None = None,
    queue_factory: Callable[..., EnrichmentQueue] | None = None,
) -> RunSummary:
    search_groups = search_groups or config.SEARCH_GROUPS
    sources = list(sources or config.SOURCES)
    validate_sources(sources)
    scraper = scraper or default_scraper
    enrichment_processor = enrichment_processor or process_enrichment
    exporter = exporter or export_all
    queue_factory = queue_factory or EnrichmentQueue
    delay = config.QUERY_DELAY_SECONDS if query_delay_seconds is None else query_delay_seconds
    source_timeout = (
        config.SOURCE_TIMEOUT_SECONDS
        if source_timeout_seconds is None
        else source_timeout_seconds
    )
    export_dir = Path(export_dir or config.EXPORT_DIR)
    verbose_logging = config.VERBOSE_LOGGING if verbose_logging is None else verbose_logging
    _configure_verbose_logging(verbose_logging)
    run_started = datetime.now()
    run_id = store.start_run(lookback_hours)

    raw_count = accepted_count = excluded_count = unmatched_count = 0
    duplicate_count = 0
    new_job_ids: list[int] = []
    errors: list[str] = []
    fetch_tasks_created = analysis_tasks_created = 0
    terms = [
        (group, term)
        for group, group_terms in search_groups.items()
        for term in group_terms
    ]
    finalized = False

    def finalize(status: str, final_errors: list[str]) -> None:
        nonlocal finalized
        if finalized:
            return
        store.finish_run(
            run_id,
            status=status,
            raw_count=raw_count,
            accepted_count=accepted_count,
            excluded_count=excluded_count,
            unmatched_count=unmatched_count,
            duplicate_count=duplicate_count,
            new_count=len(set(new_job_ids)),
            errors=final_errors,
        )
        finalized = True

    try:
        queue = queue_factory(store, migrate_existing=False)
    except BaseException as error:
        message = f"queue initialization: {type(error).__name__}: {error}"
        finalize(
            "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
            [message],
        )
        raise

    def store_attempt(result: SourceResult, query_group: str) -> None:
        nonlocal raw_count, accepted_count, excluded_count, unmatched_count
        nonlocal duplicate_count, fetch_tasks_created, analysis_tasks_created
        request = result.request
        source_error = result.error
        if result.error:
            message = (
                f"{request.source}/{query_group}/{request.search_term}: "
                f"{result.error}"
            )
            errors.append(message)
            if result.jobs.empty:
                store.record_attempt(
                    run_id,
                    request.source,
                    query_group,
                    request.search_term,
                    error=result.error,
                )
                print(f"Search failed: {message}", file=sys.stderr)
                if verbose_logging:
                    LOGGER.info(
                        "scrape: run=%s source=%s term=%r outcome=failed error=%s duration_ms=%d",
                        run_id,
                        request.source,
                        request.search_term,
                        result.error,
                        result.duration_ms,
                    )
                return
            print(f"Search warning: {message}", file=sys.stderr)

        jobs = result.jobs
        if verbose_logging:
            LOGGER.info(
                "scrape: run=%s source=%s term=%r outcome=completed results=%s duration_ms=%d",
                run_id,
                request.source,
                request.search_term,
                len(jobs),
                result.duration_ms,
            )
        accepted, stats = filter_jobs(jobs)
        raw_count += stats.raw
        accepted_count += stats.accepted
        excluded_count += stats.excluded
        unmatched_count += stats.unmatched
        store.record_attempt(
            run_id,
            request.source,
            query_group,
            request.search_term,
            stats.raw,
            stats.accepted,
            stats.excluded,
            stats.unmatched,
            error=source_error,
        )

        for row in accepted.to_dict(orient="records"):
            row["site"] = row.get("site") or request.source
            job_id, is_new = store.upsert_job(
                row,
                query_group=query_group,
                search_term=request.search_term,
            )
            queued = queue.sync_job(job_id, run_id)
            fetch_tasks_created += queued["fetch_created"]
            analysis_tasks_created += queued["analysis_created"]
            if is_new:
                new_job_ids.append(job_id)
            else:
                duplicate_count += 1
            if verbose_logging:
                LOGGER.info(
                    "scrape: run=%s job_id=%s source=%s action=stored new=%s title=%r company=%r",
                    run_id,
                    job_id,
                    request.source,
                    is_new,
                    row.get("title", ""),
                    row.get("company", ""),
                )

    try:
        parallel_indeed = "indeed" in sources and config.INDEED_MAX_WORKERS > 1
        serial_sources = [
            source for source in sources if source != "indeed" or not parallel_indeed
        ]
        indeed_futures: dict[tuple[str, str], Future[SourceResult]] = {}

        with ThreadPoolExecutor(
            max_workers=config.INDEED_MAX_WORKERS,
            thread_name_prefix="indeed-search",
        ) as executor:
            if parallel_indeed:
                for query_group, search_term in terms:
                    request = SourceRequest("indeed", search_term, lookback_hours)
                    indeed_futures[(query_group, search_term)] = executor.submit(
                        run_source_attempt,
                        request,
                        scraper=scraper,
                        timeout_seconds=source_timeout,
                    )

            # LinkedIn, Glassdoor, and future non-Indeed adapters remain in one
            # serial lane so adding a source cannot increase their request rate.
            for term_index, (query_group, search_term) in enumerate(terms):
                for source in serial_sources:
                    request = SourceRequest(source, search_term, lookback_hours)
                    store_attempt(
                        run_source_attempt(
                            request,
                            scraper=scraper,
                            timeout_seconds=source_timeout,
                        ),
                        query_group,
                    )
                if serial_sources and delay and term_index < len(terms) - 1:
                    sleeper(delay)

            # Consume Indeed results in deterministic query order even though
            # the source requests ran concurrently.
            for query_group, search_term in terms:
                future = indeed_futures.get((query_group, search_term))
                if future is not None:
                    store_attempt(future.result(), query_group)
    except BaseException as error:
        finalize(
            "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
            [*errors, f"scraping: {type(error).__name__}: {error}"],
        )
        raise

    unique_new_ids = list(dict.fromkeys(new_job_ids))
    status = "completed_with_errors" if errors else "completed"
    if errors and len(errors) == len(terms) * len(sources):
        status = "failed"
    enrichment = EnrichmentStats()
    fatal_error: Exception | None = None
    if process_queues:
        try:
            enrichment = enrichment_processor(
                queue,
                current_run_id=run_id,
                verbose_logging=verbose_logging,
                max_seconds=config.ENRICHMENT_MAX_SECONDS,
                api_call_limit=config.LLM_MAX_CALLS_PER_CYCLE,
            )
            fetch_tasks_created += enrichment.orphan_fetch_created
            analysis_tasks_created += (
                enrichment.orphan_analysis_created
                + enrichment.fetch.analysis_created
            )
        except KeyboardInterrupt as error:
            finalize(
                "interrupted",
                [*errors, f"enrichment: {type(error).__name__}: {error}"],
            )
            raise
        except Exception as error:
            fatal_error = error
            errors.append(f"enrichment: {type(error).__name__}: {error}")
            enrichment.fetch.errors.append(f"Enrichment worker failed: {error}")
    try:
        store.record_enrichment_summary(
            run_id,
            fetch_created=fetch_tasks_created,
            fetch_completed=enrichment.fetch.completed,
            fetch_retried=enrichment.fetch.retried,
            fetch_dead=enrichment.fetch.dead,
            analysis_created=analysis_tasks_created,
            analysis_completed=enrichment.analysis.completed,
            analysis_retried=enrichment.analysis.retried,
            analysis_blocked=enrichment.analysis.budget_blocked,
            analysis_dead=enrichment.analysis.dead,
            calls=enrichment.analysis.calls,
            input_tokens=enrichment.analysis.input_tokens,
            output_tokens=enrichment.analysis.output_tokens,
            estimated_cost=enrichment.analysis.estimated_cost,
            errors=[*enrichment.fetch.errors, *enrichment.analysis.errors],
            circuit_breakers=[
                enrichment.fetch.circuit_breaker,
                enrichment.analysis.circuit_breaker,
            ],
        )
    except KeyboardInterrupt as error:
        finalize(
            "interrupted",
            [*errors, f"enrichment summary: {type(error).__name__}: {error}"],
        )
        raise
    except Exception as error:
        if fatal_error is None:
            fatal_error = error
        errors.append(f"enrichment summary: {type(error).__name__}: {error}")
    try:
        current_export, new_export = exporter(
            store,
            export_dir,
            unique_new_ids,
            run_started,
        )
    except BaseException as error:
        export_error = f"export: {type(error).__name__}: {error}"
        finalize(
            "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
            [*errors, export_error],
        )
        raise
    if fatal_error is not None:
        finalize("failed", errors)
        raise RuntimeError(
            "Scrape cycle failed during enrichment; exports were written"
        ) from fatal_error
    finalize(status, errors)
    return RunSummary(
        run_id,
        raw_count,
        accepted_count,
        excluded_count,
        unmatched_count,
        duplicate_count,
        tuple(unique_new_ids),
        tuple(errors),
        current_export,
        new_export,
        enrichment,
        fetch_tasks_created,
        analysis_tasks_created,
    )
