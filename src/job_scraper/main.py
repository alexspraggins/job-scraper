"""Multi-query scraper orchestration and command-line interface."""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
import logging
import multiprocessing
from pathlib import Path
from queue import Empty
import sys
import time
from typing import Callable, Sequence

import pandas as pd
from jobspy import scrape_jobs

from . import config
from .emailer import send_email
from .enrichment import EnrichmentStats, process_enrichment
from .filtering import filter_jobs
from .output import export_all
from .skillsire import scrape_skillsire
from .storage import JobStore, VALID_STATUSES
from .queueing import EnrichmentQueue, TASK_STATUSES


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


def _scrape_source(
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
) -> pd.DataFrame:
    if source == "skillsire":
        empty = pd.DataFrame(columns=["job_url", "title", "company", "location"])
        result = scrape_skillsire(
            empty,
            hours=lookback_hours,
            search_term=search_term,
            results_fetch_count=config.RESULTS_PER_QUERY,
        )
        if not result.empty:
            result = result.copy()
            result["site"] = "skillsire"
        return result

    return scraper(
        site_name=[source],
        search_term=search_term,
        location=config.LOCATION,
        results_wanted=config.RESULTS_PER_QUERY,
        hours_old=lookback_hours,
        country_indeed=config.COUNTRY,
    )


def _scrape_worker(
    result_queue,
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
) -> None:
    try:
        result_queue.put(
            ("ok", _scrape_source(source, search_term, lookback_hours, scraper))
        )
    except Exception as error:
        result_queue.put(("error", f"{type(error).__name__}: {error}"))


def _scrape_source_with_timeout(
    source: str,
    search_term: str,
    lookback_hours: int,
    scraper: Callable[..., pd.DataFrame],
    timeout_seconds: float,
) -> pd.DataFrame:
    if timeout_seconds <= 0:
        return _scrape_source(source, search_term, lookback_hours, scraper)

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
    return payload


def run_scrape_cycle(
    store: JobStore,
    lookback_hours: int,
    *,
    search_groups: dict[str, list[str]] | None = None,
    sources: list[str] | None = None,
    scraper: Callable[..., pd.DataFrame] = scrape_jobs,
    query_delay_seconds: float | None = None,
    source_timeout_seconds: float | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    export_dir: str | Path | None = None,
    process_queues: bool = True,
    verbose_logging: bool | None = None,
) -> RunSummary:
    search_groups = search_groups or config.SEARCH_GROUPS
    sources = list(sources or config.SOURCES)
    if config.SKILLSIRE_ENABLED and "skillsire" not in sources:
        sources.append("skillsire")
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
    queue = EnrichmentQueue(store, migrate_existing=False)

    raw_count = accepted_count = excluded_count = unmatched_count = 0
    duplicate_count = 0
    new_job_ids: list[int] = []
    errors: list[str] = []
    fetch_tasks_created = analysis_tasks_created = 0
    terms = [(group, term) for group, group_terms in search_groups.items() for term in group_terms]

    def store_attempt(
        source: str,
        query_group: str,
        search_term: str,
        result: Callable[[], pd.DataFrame],
    ) -> None:
        nonlocal raw_count, accepted_count, excluded_count, unmatched_count
        nonlocal duplicate_count, fetch_tasks_created, analysis_tasks_created
        started = time.perf_counter()
        try:
            jobs = result()
            if jobs is None:
                jobs = pd.DataFrame()
            if verbose_logging:
                LOGGER.info(
                    "scrape: run=%s source=%s term=%r outcome=completed results=%s duration_ms=%d",
                    run_id, source, search_term, len(jobs),
                    round((time.perf_counter() - started) * 1000),
                )
            accepted, stats = filter_jobs(jobs)
            raw_count += stats.raw
            accepted_count += stats.accepted
            excluded_count += stats.excluded
            unmatched_count += stats.unmatched
            store.record_attempt(
                run_id,
                source,
                query_group,
                search_term,
                stats.raw,
                stats.accepted,
                stats.excluded,
                stats.unmatched,
            )

            for row in accepted.to_dict(orient="records"):
                row["site"] = row.get("site") or source
                job_id, is_new = store.upsert_job(
                    row,
                    query_group=query_group,
                    search_term=search_term,
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
                        run_id, job_id, source, is_new,
                        row.get("title", ""), row.get("company", ""),
                    )
        except Exception as error:
            message = f"{source}/{query_group}/{search_term}: {error}"
            errors.append(message)
            store.record_attempt(
                run_id,
                source,
                query_group,
                search_term,
                error=str(error),
            )
            print(f"Search failed: {message}", file=sys.stderr)
            if verbose_logging:
                LOGGER.info(
                    "scrape: run=%s source=%s term=%r outcome=failed error=%s duration_ms=%d",
                    run_id, source, search_term, type(error).__name__,
                    round((time.perf_counter() - started) * 1000),
                )

    try:
        parallel_indeed = "indeed" in sources and config.INDEED_MAX_WORKERS > 1
        serial_sources = [
            source for source in sources if source != "indeed" or not parallel_indeed
        ]
        indeed_futures: dict[tuple[str, str], Future[pd.DataFrame]] = {}

        with ThreadPoolExecutor(
            max_workers=config.INDEED_MAX_WORKERS,
            thread_name_prefix="indeed-search",
        ) as executor:
            if parallel_indeed:
                for query_group, search_term in terms:
                    indeed_futures[(query_group, search_term)] = executor.submit(
                        _scrape_source_with_timeout,
                        "indeed",
                        search_term,
                        lookback_hours,
                        scraper,
                        source_timeout,
                    )

            # LinkedIn and optional sources stay in one serial lane. The delay
            # applies here, so parallel Indeed work cannot increase LinkedIn's
            # request rate.
            for term_index, (query_group, search_term) in enumerate(terms):
                for source in serial_sources:
                    store_attempt(
                        source,
                        query_group,
                        search_term,
                        lambda source=source, search_term=search_term: (
                            _scrape_source_with_timeout(
                                source,
                                search_term,
                                lookback_hours,
                                scraper,
                                source_timeout,
                            )
                        ),
                    )
                if serial_sources and delay and term_index < len(terms) - 1:
                    sleeper(delay)

            # Consume results in deterministic query order even though the
            # network requests ran concurrently.
            for query_group, search_term in terms:
                future = indeed_futures.get((query_group, search_term))
                if future is not None:
                    store_attempt(
                        "indeed",
                        query_group,
                        search_term,
                        future.result,
                    )
    except BaseException as error:
        store.finish_run(
            run_id,
            status="interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
            raw_count=raw_count,
            accepted_count=accepted_count,
            excluded_count=excluded_count,
            unmatched_count=unmatched_count,
            duplicate_count=duplicate_count,
            new_count=len(set(new_job_ids)),
            errors=[*errors, f"{type(error).__name__}: {error}"],
        )
        raise

    unique_new_ids = list(dict.fromkeys(new_job_ids))
    status = "completed_with_errors" if errors else "completed"
    if errors and len(errors) == len(terms) * len(sources):
        status = "failed"
    enrichment = EnrichmentStats()
    if process_queues:
        try:
            enrichment = process_enrichment(
                queue, current_run_id=run_id, verbose_logging=verbose_logging
            )
        except Exception as error:
            # Queue work is deliberately best-effort; scrape data and exports survive.
            enrichment.fetch.errors.append(f"Enrichment worker failed: {error}")
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
        circuit_breakers=[enrichment.fetch.circuit_breaker,
                          enrichment.analysis.circuit_breaker],
    )
    current_export, new_export = export_all(
        store,
        export_dir,
        unique_new_ids,
        run_started,
    )
    # A run is complete only after enrichment and exports finish. This makes
    # finished_at represent the full cycle instead of the source searches alone.
    store.finish_run(
        run_id,
        status=status,
        raw_count=raw_count,
        accepted_count=accepted_count,
        excluded_count=excluded_count,
        unmatched_count=unmatched_count,
        duplicate_count=duplicate_count,
        new_count=len(unique_new_ids),
        errors=errors,
    )
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


def _print_summary(summary: RunSummary) -> None:
    print(
        f"Run {summary.run_id}: raw={summary.raw_count}, "
        f"accepted={summary.accepted_count}, excluded={summary.excluded_count}, "
        f"unmatched={summary.unmatched_count}, duplicates={summary.duplicate_count}, "
        f"new={len(summary.new_job_ids)}, errors={len(summary.errors)}"
    )
    if summary.new_job_ids:
        print(f"New jobs: {summary.new_export}")
    elif summary.raw_count == 0:
        print("No new jobs: sources returned no raw listings.")
    elif summary.accepted_count == 0:
        print("No new jobs: all raw listings were excluded or unmatched.")
    else:
        print("No new jobs: all accepted listings were already stored.")
    print(f"Current jobs: {summary.current_export}")
    fetch = summary.enrichment.fetch
    analysis = summary.enrichment.analysis
    print(
        f"Enrichment: fetch created={summary.fetch_tasks_created}, "
        f"completed={fetch.completed}, retry={fetch.retried}, dead={fetch.dead}; "
        f"analysis created={summary.analysis_tasks_created}, completed={analysis.completed}, "
        f"retry={analysis.retried}, blocked={analysis.budget_blocked}, dead={analysis.dead}; "
        f"OpenAI calls={analysis.calls}, tokens={analysis.input_tokens + analysis.output_tokens}, "
        f"cost=${analysis.estimated_cost:.6f}"
    )
    for error in [*fetch.errors, *analysis.errors]:
        print(f"Enrichment warning: {error}", file=sys.stderr)


def _email_new_jobs(summary: RunSummary) -> None:
    if not config.EMAIL_ENABLED or summary.new_export is None:
        return
    required = {
        "JOB_SCRAPER_EMAIL_FROM": config.EMAIL_FROM,
        "JOB_SCRAPER_EMAIL_PASSWORD": config.EMAIL_PASSWORD,
        "JOB_SCRAPER_EMAIL_TO": config.EMAIL_TO,
        "JOB_SCRAPER_EMAIL_SMTP": config.EMAIL_SMTP,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing email settings: {', '.join(missing)}")
    send_email(
        str(summary.new_export),
        config.EMAIL_TO,
        config.EMAIL_FROM,
        config.EMAIL_PASSWORD,
        config.EMAIL_SMTP,
    )


def run_command(args: argparse.Namespace) -> int:
    store = JobStore(args.database)
    lookback = args.lookback_hours or config.INITIAL_LOOKBACK_HOURS
    try:
        while True:
            print(f"Starting scrape at {datetime.now():%Y-%m-%d %H:%M:%S}")
            summary = run_scrape_cycle(
                store, lookback, export_dir=args.output_dir,
                verbose_logging=args.verbose or config.VERBOSE_LOGGING,
            )
            _print_summary(summary)
            _email_new_jobs(summary)
            if args.once:
                return 0 if not summary.errors else 1
            lookback = config.RECURRING_LOOKBACK_HOURS
            print(f"Sleeping for {config.POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(config.POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Scraping stopped by user.")
        return 0


def list_command(args: argparse.Namespace) -> int:
    store = JobStore(args.database)
    jobs = store.list_jobs(args.status, args.limit)
    if not jobs:
        print("No jobs found.")
        return 0
    print(f"{'ID':>5}  {'STATUS':<9}  {'TITLE':<45}  COMPANY")
    for job in jobs:
        title = (job["title"] or "")[:45]
        company = job["company"] or ""
        print(f"{job['id']:>5}  {job['status']:<9}  {title:<45}  {company}")
    return 0


def status_command(args: argparse.Namespace) -> int:
    store = JobStore(args.database)
    try:
        store.set_status(args.job_id, args.new_status, args.note)
    except (KeyError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Job {args.job_id} marked {args.new_status}.")
    return 0


def export_command(args: argparse.Namespace) -> int:
    store = JobStore(args.database)
    current_export, _ = export_all(store, args.output_dir)
    print(f"Exported jobs to {current_export}")
    return 0


def queue_command(args: argparse.Namespace) -> int:
    queue = EnrichmentQueue(JobStore(args.database), migrate_existing=False)
    if args.status:
        tasks = queue.list_tasks(args.status, args.limit)
        if not tasks:
            print("No matching enrichment tasks.")
            return 0
        print(f"{'ID':>5}  {'TYPE':<20} {'STATUS':<14} {'TRY':>3} {'JOB':>5}  ERROR")
        for task in tasks:
            error = task["last_error_message"] or ""
            print(f"{task['id']:>5}  {task['task_type']:<20} {task['status']:<14} "
                  f"{task['attempt_count']:>3} {task['job_id']:>5}  {error[:60]}")
    else:
        rows = queue.queue_summary()
        if not rows:
            print("Enrichment queue is empty.")
        for row in rows:
            print(f"{row['task_type']:<20} {row['status']:<14} {row['count']:>5}")
    return 0


def retry_command(args: argparse.Namespace) -> int:
    queue = EnrichmentQueue(JobStore(args.database), migrate_existing=False)
    try:
        count = queue.retry(task_id=args.task_id, job_id=args.job_id)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    if not count:
        print("No dead tasks matched.", file=sys.stderr)
        return 2
    print(f"Reset {count} task(s) to pending.")
    return 0


def enrich_command(args: argparse.Namespace) -> int:
    queue = EnrichmentQueue(
        JobStore(args.database), migrate_existing=args.migrate_existing
    )
    verbose_logging = args.verbose or config.VERBOSE_LOGGING
    _configure_verbose_logging(verbose_logging)
    stats = process_enrichment(
        queue, fetch_limit=args.limit, analysis_limit=args.limit,
        fetch_only=args.fetch_only, verbose_logging=verbose_logging,
    )
    print(f"Fetch: completed={stats.fetch.completed}, retry={stats.fetch.retried}, "
          f"dead={stats.fetch.dead}")
    if not args.fetch_only:
        print(f"Analysis: completed={stats.analysis.completed}, retry={stats.analysis.retried}, "
              f"blocked={stats.analysis.budget_blocked}, dead={stats.analysis.dead}, "
              f"calls={stats.analysis.calls}")
    return 0


def show_command(args: argparse.Namespace) -> int:
    store = JobStore(args.database)
    try:
        details = store.get_job_details(args.job_id)
    except KeyError as error:
        print(str(error), file=sys.stderr)
        return 2
    job = details["job"]
    print(f"Job {job['id']}: {job['title']} — {job['company'] or ''}")
    print(f"Status: {job['status']}  Location: {job['location'] or ''}")
    for posting in details["postings"]:
        print(f"Posting {posting['id']} ({posting['source']}): {posting['url'] or ''}")
    if not details["requirements"]:
        print("Requirements: not analyzed")
    for requirement in details["requirements"]:
        print(f"- {requirement['requirement_type']} [{requirement['priority']}]: "
              f"{requirement['canonical_value']} — {requirement['evidence']}")
    return 0


def usage_command(args: argparse.Namespace) -> int:
    queue = EnrichmentQueue(JobStore(args.database))
    if args.resolve:
        if args.actual_cost is None:
            print("--actual-cost is required with --resolve", file=sys.stderr)
            return 2
        try:
            queue.resolve_usage(args.resolve, args.actual_cost)
        except (KeyError, ValueError) as error:
            print(str(error), file=sys.stderr)
            return 2
        print(f"Resolved reservation {args.resolve} at ${args.actual_cost:.6f}.")
    if args.outstanding:
        rows = queue.outstanding_usage()
        if not rows:
            print("No outstanding reservations.")
        for row in rows:
            print(f"{row['reservation_token']} task={row['task_id']} month={row['month_key']} "
                  f"status={row['status']} projected=${row['projected_cost_usd']:.6f}")
    usage = queue.usage(args.month)
    remaining = max(0.0, config.LLM_MONTHLY_BUDGET_USD - usage["cost"])
    print(f"{usage['month']}: calls={usage['calls']}, input={usage['input_tokens']}, "
          f"output={usage['output_tokens']}, counted=${usage['cost']:.6f}, "
          f"remaining=${remaining:.6f}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect and track job listings.")
    parser.add_argument("--database", type=Path, default=config.DATABASE_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.EXPORT_DIR)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run job searches")
    run_parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    run_parser.add_argument("--lookback-hours", type=int)
    run_parser.add_argument("--verbose", action="store_true",
                            help="Log scrape and enrichment timing details")
    run_parser.set_defaults(handler=run_command)

    list_parser = subparsers.add_parser("list", help="List stored jobs")
    list_parser.add_argument("--status", choices=VALID_STATUSES)
    list_parser.add_argument("--limit", type=int, default=50)
    list_parser.set_defaults(handler=list_command)

    status_parser = subparsers.add_parser("status", help="Update a job status")
    status_parser.add_argument("job_id", type=int)
    status_parser.add_argument("new_status", choices=VALID_STATUSES)
    status_parser.add_argument("--note")
    status_parser.set_defaults(handler=status_command)

    export_parser = subparsers.add_parser("export", help="Regenerate the current CSV")
    export_parser.set_defaults(handler=export_command)

    queue_parser = subparsers.add_parser("queue", help="Inspect enrichment tasks")
    queue_parser.add_argument("--status", choices=TASK_STATUSES)
    queue_parser.add_argument("--limit", type=int, default=50)
    queue_parser.set_defaults(handler=queue_command)

    retry_parser = subparsers.add_parser("retry", help="Retry dead enrichment tasks")
    retry_parser.add_argument("task_id", type=int, nargs="?")
    retry_parser.add_argument("--job-id", type=int)
    retry_parser.set_defaults(handler=retry_command)

    enrich_parser = subparsers.add_parser("enrich", help="Process due enrichment work")
    enrich_parser.add_argument("--limit", type=int, default=20)
    enrich_parser.add_argument("--fetch-only", action="store_true")
    enrich_parser.add_argument(
        "--migrate-existing", action="store_true",
        help="Backfill missing enrichment tasks for all eligible stored jobs",
    )
    enrich_parser.add_argument("--verbose", action="store_true",
                              help="Log enrichment timing details")
    enrich_parser.set_defaults(handler=enrich_command)

    show_parser = subparsers.add_parser("show", help="Show a job and extracted requirements")
    show_parser.add_argument("job_id", type=int)
    show_parser.set_defaults(handler=show_command)

    usage_parser = subparsers.add_parser("usage", help="Show monthly OpenAI usage")
    usage_parser.add_argument("--month", help="UTC month in YYYY-MM format")
    usage_parser.add_argument("--outstanding", action="store_true",
                              help="List reserved or uncertain API calls")
    usage_parser.add_argument("--resolve", metavar="TOKEN",
                              help="Resolve an outstanding usage reservation")
    usage_parser.add_argument("--actual-cost", type=float,
                              help="Actual USD cost used with --resolve")
    usage_parser.set_defaults(handler=usage_command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = list(argv) if argv is not None else sys.argv[1:]
    if not arguments:
        arguments = ["run"]
    args = parser.parse_args(arguments)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
