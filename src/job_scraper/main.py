"""Multi-query scraper orchestration and command-line interface."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
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
from .filtering import filter_jobs
from .output import export_all
from .skillsire import scrape_skillsire
from .storage import JobStore, VALID_STATUSES


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
    run_started = datetime.now()
    run_id = store.start_run(lookback_hours)

    raw_count = accepted_count = excluded_count = unmatched_count = 0
    duplicate_count = 0
    new_job_ids: list[int] = []
    errors: list[str] = []
    terms = [(group, term) for group, group_terms in search_groups.items() for term in group_terms]

    try:
        for term_index, (query_group, search_term) in enumerate(terms):
            for source in sources:
                try:
                    jobs = _scrape_source_with_timeout(
                        source,
                        search_term,
                        lookback_hours,
                        scraper,
                        source_timeout,
                    )
                    if jobs is None:
                        jobs = pd.DataFrame()
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
                        if is_new:
                            new_job_ids.append(job_id)
                        else:
                            duplicate_count += 1
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

            if delay and term_index < len(terms) - 1:
                sleeper(delay)
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
    current_export, new_export = export_all(
        store,
        export_dir,
        unique_new_ids,
        run_started,
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
            summary = run_scrape_cycle(store, lookback, export_dir=args.output_dir)
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect and track job listings.")
    parser.add_argument("--database", type=Path, default=config.DATABASE_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.EXPORT_DIR)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run job searches")
    run_parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    run_parser.add_argument("--lookback-hours", type=int)
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
