"""Command-line interface for the job scraper."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys
import time
from typing import Sequence

from . import config
from .emailer import send_email
from .enrichment import process_enrichment
from .output import export_all
from .pipeline import (
    RunSummary,
    _configure_verbose_logging,
    run_scrape_cycle as _run_scrape_cycle,
)
from .queueing import EnrichmentQueue, TASK_STATUSES
from .sources import scrape_source_with_timeout as _scrape_source_with_timeout
from .storage import JobStore, VALID_STATUSES


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def run_scrape_cycle(*args, **kwargs) -> RunSummary:
    """Compatibility wrapper for the pipeline's public scrape entry point."""
    kwargs.setdefault("enrichment_processor", process_enrichment)
    kwargs.setdefault("exporter", export_all)
    kwargs.setdefault("queue_factory", EnrichmentQueue)
    return _run_scrape_cycle(*args, **kwargs)


def _print_summary(summary: RunSummary, *, enrichment_skipped: bool = False) -> None:
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
    if enrichment_skipped:
        print(
            "Enrichment: skipped. Jobs were filtered and exported; "
            "run `uv run job-scraper enrich` later if needed."
        )
        return

    fetch = summary.enrichment.fetch
    analysis = summary.enrichment.analysis
    print(
        f"Enrichment: fetch created={summary.fetch_tasks_created}, "
        f"claimed={fetch.claimed}, completed={fetch.completed}, retry={fetch.retried}, dead={fetch.dead}; "
        f"analysis created={summary.analysis_tasks_created}, completed={analysis.completed}, "
        f"claimed={analysis.claimed}, retry={analysis.retried}, blocked={analysis.budget_blocked}, dead={analysis.dead}; "
        f"repairs={analysis.repair_attempts}, elapsed={summary.enrichment.elapsed_seconds:.1f}s; "
        f"OpenAI calls={analysis.calls}, tokens={analysis.input_tokens + analysis.output_tokens}, "
        f"cost=${analysis.estimated_cost:.6f}"
    )
    if summary.enrichment.deadline_reached:
        print("Enrichment window reached; remaining work stays queued.")
    if summary.enrichment.remaining_tasks:
        remaining = ", ".join(
            f"{key}={value}"
            for key, value in sorted(summary.enrichment.remaining_tasks.items())
        )
        print(f"Enrichment remaining: {remaining}")
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
                store,
                lookback,
                export_dir=args.output_dir,
                verbose_logging=args.verbose or config.VERBOSE_LOGGING,
                process_queues=not args.no_enrichment,
            )
            _print_summary(summary, enrichment_skipped=args.no_enrichment)
            _email_new_jobs(summary)
            if args.once:
                return 0 if not summary.errors else 1
            lookback = config.RECURRING_LOOKBACK_HOURS
            print(f"Sleeping for {config.POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(config.POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Scraping stopped by user.")
        return 0
    except Exception as error:
        print(f"Scrape failed: {error}", file=sys.stderr)
        return 1


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
            print(
                f"{task['id']:>5}  {task['task_type']:<20} {task['status']:<14} "
                f"{task['attempt_count']:>3} {task['job_id']:>5}  {error[:60]}"
            )
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
    queue = EnrichmentQueue(JobStore(args.database), migrate_existing=False)
    if args.migrate_existing:
        migrated = queue.migrate_existing()
        print(
            f"Migration: fetch created={migrated['fetch_created']}, "
            f"analysis created={migrated['analysis_created']}, "
            f"cancelled={migrated['cancelled']}"
        )
    verbose_logging = args.verbose or config.VERBOSE_LOGGING
    _configure_verbose_logging(verbose_logging)
    stats = process_enrichment(
        queue,
        fetch_limit=args.limit,
        analysis_limit=args.limit,
        fetch_only=args.fetch_only,
        verbose_logging=verbose_logging,
    )
    if stats.orphan_fetch_created or stats.orphan_analysis_created:
        print(
            f"Orphan repair: fetch created={stats.orphan_fetch_created}, "
            f"analysis created={stats.orphan_analysis_created}"
        )
    print(
        f"Fetch: claimed={stats.fetch.claimed}, completed={stats.fetch.completed}, "
        f"retry={stats.fetch.retried}, dead={stats.fetch.dead}"
    )
    if not args.fetch_only:
        print(
            f"Analysis: claimed={stats.analysis.claimed}, completed={stats.analysis.completed}, "
            f"retry={stats.analysis.retried}, blocked={stats.analysis.budget_blocked}, "
            f"dead={stats.analysis.dead}, repairs={stats.analysis.repair_attempts}, "
            f"calls={stats.analysis.calls}"
        )
    print(f"Enrichment elapsed: {stats.elapsed_seconds:.1f}s")
    if stats.remaining_tasks:
        remaining = ", ".join(
            f"{key}={value}" for key, value in sorted(stats.remaining_tasks.items())
        )
        print(f"Enrichment remaining: {remaining}")
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
        description = posting["description_source"] or (
            "stored" if posting["description_available"] else "unavailable"
        )
        print(
            f"Posting {posting['posting_id']} ({posting['source']}): "
            f"{posting['url'] or ''}"
        )
        print(
            f"  Analysis: {posting['analysis_status']}  Description: {description}"
        )
        if posting["task_id"]:
            print(
                f"  Task {posting['task_id']}: {posting['task_type']} "
                f"status={posting['task_status']} attempts={posting['attempt_count']}"
            )
        if posting["last_error_message"]:
            print(
                f"  Last error: {posting['last_error_class'] or 'Error'}: "
                f"{posting['last_error_message']}"
            )
    if not details["requirements"]:
        print("Requirements: not analyzed")
    for requirement in details["requirements"]:
        print(
            f"- posting {requirement['posting_id']} ({requirement['source']}) "
            f"{requirement['requirement_type']} [{requirement['priority']}]: "
            f"{requirement['canonical_value']} — {requirement['evidence']}"
        )
    return 0


def usage_command(args: argparse.Namespace) -> int:
    queue = EnrichmentQueue(JobStore(args.database), migrate_existing=False)
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
            print(
                f"{row['reservation_token']} task={row['task_id']} month={row['month_key']} "
                f"status={row['status']} projected=${row['projected_cost_usd']:.6f}"
            )
    usage = queue.usage(args.month)
    remaining = max(0.0, config.LLM_MONTHLY_BUDGET_USD - usage["cost"])
    print(
        f"{usage['month']}: calls={usage['calls']}, input={usage['input_tokens']}, "
        f"output={usage['output_tokens']}, counted=${usage['cost']:.6f}, "
        f"remaining=${remaining:.6f}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect and track job listings.")
    parser.add_argument("--database", type=Path, default=config.DATABASE_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.EXPORT_DIR)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run job searches")
    run_parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    run_parser.add_argument("--lookback-hours", type=int)
    run_parser.add_argument(
        "--no-enrichment", "--skip-enrichment", dest="no_enrichment",
        action="store_true",
        help="Fetch, filter, store, and export jobs without processing enrichment",
    )
    run_parser.add_argument(
        "--verbose", action="store_true", help="Log scrape and enrichment timing details"
    )
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
    enrich_parser.add_argument("--limit", type=_positive_int, default=20)
    enrich_parser.add_argument("--fetch-only", action="store_true")
    enrich_parser.add_argument(
        "--migrate-existing",
        action="store_true",
        help="Backfill missing enrichment tasks for all eligible stored jobs",
    )
    enrich_parser.add_argument(
        "--verbose", action="store_true", help="Log enrichment timing details"
    )
    enrich_parser.set_defaults(handler=enrich_command)

    show_parser = subparsers.add_parser("show", help="Show a job and extracted requirements")
    show_parser.add_argument("job_id", type=int)
    show_parser.set_defaults(handler=show_command)

    usage_parser = subparsers.add_parser("usage", help="Show monthly OpenAI usage")
    usage_parser.add_argument("--month", help="UTC month in YYYY-MM format")
    usage_parser.add_argument("--outstanding", action="store_true", help="List reserved or uncertain API calls")
    usage_parser.add_argument("--resolve", metavar="TOKEN", help="Resolve an outstanding usage reservation")
    usage_parser.add_argument("--actual-cost", type=float, help="Actual USD cost used with --resolve")
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
