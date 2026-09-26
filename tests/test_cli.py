import argparse
from pathlib import Path

import pytest

import job_scraper.main as main_module
from job_scraper.enrichment import EnrichmentStats
from job_scraper.main import build_parser, main


def parse(*arguments: str) -> argparse.Namespace:
    return build_parser().parse_args(list(arguments))


def test_parser_supports_global_and_run_options():
    args = parse(
        "--database", "jobs.sqlite3",
        "--output-dir", "exports",
        "run",
        "--once",
        "--lookback-hours", "6",
        "--no-enrichment",
        "--verbose",
    )

    assert args.database == Path("jobs.sqlite3")
    assert args.output_dir == Path("exports")
    assert args.command == "run"
    assert args.once is True
    assert args.lookback_hours == 6
    assert args.no_enrichment is True
    assert args.verbose is True
    assert args.handler is main_module.run_command


@pytest.mark.parametrize("flag", ["--no-enrichment", "--skip-enrichment"])
def test_parser_treats_enrichment_flags_as_aliases(flag):
    args = parse("run", flag)

    assert args.no_enrichment is True


def test_parser_supports_review_queue_retry_enrichment_and_usage_options():
    listing = parse("list", "--status", "saved", "--limit", "12")
    assert listing.status == "saved"
    assert listing.limit == 12
    assert listing.handler is main_module.list_command

    status = parse("status", "42", "applied", "--note", "Follow up")
    assert status.job_id == 42
    assert status.new_status == "applied"
    assert status.note == "Follow up"

    queue = parse("queue", "--status", "pending", "--limit", "3")
    assert queue.status == "pending"
    assert queue.limit == 3

    retry_task = parse("retry", "19")
    assert retry_task.task_id == 19
    assert retry_task.job_id is None

    retry_job = parse("retry", "--job-id", "42")
    assert retry_job.task_id is None
    assert retry_job.job_id == 42

    enrich = parse(
        "enrich",
        "--limit", "7",
        "--fetch-only",
        "--migrate-existing",
        "--verbose",
    )
    assert enrich.limit == 7
    assert enrich.fetch_only is True
    assert enrich.migrate_existing is True
    assert enrich.verbose is True

    usage = parse(
        "usage",
        "--month", "2026-09",
        "--outstanding",
        "--resolve", "reservation-token",
        "--actual-cost", "0.12",
    )
    assert usage.month == "2026-09"
    assert usage.outstanding is True
    assert usage.resolve == "reservation-token"
    assert usage.actual_cost == 0.12


@pytest.mark.parametrize(
    "arguments",
    [
        ("list", "--status", "not-a-status"),
        ("queue", "--status", "not-a-task-status"),
        ("status", "42", "not-a-status"),
    ],
)
def test_parser_rejects_invalid_choice_values(arguments):
    with pytest.raises(SystemExit) as error:
        parse(*arguments)

    assert error.value.code == 2


def test_main_dispatches_run_options_without_starting_a_scrape(tmp_path, monkeypatch):
    captured = {}

    def fake_run_command(args):
        captured["args"] = args
        return 7

    monkeypatch.setattr(main_module, "run_command", fake_run_command)

    result = main([
        "--database", str(tmp_path / "jobs.sqlite3"),
        "--output-dir", str(tmp_path / "exports"),
        "run",
        "--once",
        "--lookback-hours", "8",
        "--skip-enrichment",
        "--verbose",
    ])

    assert result == 7
    args = captured["args"]
    assert args.database == tmp_path / "jobs.sqlite3"
    assert args.output_dir == tmp_path / "exports"
    assert args.once is True
    assert args.lookback_hours == 8
    assert args.no_enrichment is True
    assert args.verbose is True


def test_enrich_options_reach_the_enrichment_processor(tmp_path, monkeypatch):
    captured = {}

    def fake_process(queue, **kwargs):
        captured["queue"] = queue
        captured.update(kwargs)
        return EnrichmentStats()

    monkeypatch.setattr(main_module, "process_enrichment", fake_process)

    result = main([
        "--database", str(tmp_path / "jobs.sqlite3"),
        "enrich",
        "--limit", "7",
        "--fetch-only",
        "--migrate-existing",
        "--verbose",
    ])

    assert result == 0
    assert captured["fetch_limit"] == 7
    assert captured["analysis_limit"] == 7
    assert captured["fetch_only"] is True
    assert captured["verbose_logging"] is True


def test_usage_resolve_requires_actual_cost(tmp_path, capsys):
    result = main([
        "--database", str(tmp_path / "jobs.sqlite3"),
        "usage",
        "--resolve", "reservation-token",
    ])

    assert result == 2
    assert "--actual-cost is required with --resolve" in capsys.readouterr().err
