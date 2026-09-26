import logging
import threading
import time
from types import SimpleNamespace

import pandas as pd
import pytest

import job_scraper.main as main_module
from job_scraper.main import _scrape_source_with_timeout, main, run_scrape_cycle
from job_scraper.enrichment import EnrichmentStats, FetchError, WorkerStats
from job_scraper.queueing import EnrichmentQueue
from job_scraper.storage import JobStore


def test_verbose_scrape_logs_request_and_stored_job(tmp_path, caplog):
    def fake_scraper(**kwargs):
        return pd.DataFrame([{
            "id": "verbose-1",
            "site": "indeed",
            "job_url": "https://example.com/verbose-1",
            "title": "Associate Software Developer",
            "company": "Example",
            "location": "Boston, MA",
        }])

    store = JobStore(tmp_path / "jobs.sqlite3")
    with caplog.at_level(logging.INFO, logger="job_scraper"):
        summary = run_scrape_cycle(
            store,
            24,
            search_groups={"core_software": ["software developer"]},
            sources=["indeed"],
            scraper=fake_scraper,
            query_delay_seconds=0,
            source_timeout_seconds=0,
            export_dir=tmp_path / "exports",
            process_queues=False,
            verbose_logging=True,
        )

    messages = [record.message for record in caplog.records]
    assert any(
        "scrape:" in message
        and f"run={summary.run_id}" in message
        and "source=indeed" in message
        and "results=1" in message
        and "duration_ms=" in message
        for message in messages
    )
    assert any(
        "action=stored" in message
        and f"job_id={summary.new_job_ids[0]}" in message
        and "new=True" in message
        for message in messages
    )


def test_scrape_cycle_isolates_source_failure_and_records_counts(tmp_path):
    def fake_scraper(**kwargs):
        if kwargs["site_name"] == ["linkedin"]:
            raise RuntimeError("blocked")
        return pd.DataFrame(
            [
                {
                    "id": "1",
                    "site": "indeed",
                    "job_url": "https://example.com/1",
                    "title": "Junior Software Engineer",
                    "company": "Example",
                    "location": "Remote",
                    "date_posted": "2026-09-21",
                },
                {
                    "id": "2",
                    "site": "indeed",
                    "job_url": "https://example.com/2",
                    "title": "Senior Software Engineer",
                    "company": "Example",
                    "location": "Remote",
                },
            ]
        )

    store = JobStore(tmp_path / "jobs.sqlite3")
    summary = run_scrape_cycle(
        store,
        24,
        search_groups={"core_software": ["software engineer"]},
        sources=["indeed", "linkedin"],
        scraper=fake_scraper,
        query_delay_seconds=0,
        source_timeout_seconds=0,
        export_dir=tmp_path / "exports",
    )

    assert summary.raw_count == 2
    assert summary.accepted_count == 1
    assert summary.excluded_count == 1
    assert len(summary.new_job_ids) == 1
    assert len(summary.errors) == 1
    assert store.get_run(summary.run_id)["status"] == "completed_with_errors"


def test_scrape_cycle_reports_jobspy_logged_source_failure(tmp_path):
    def fake_scraper(**kwargs):
        if kwargs["site_name"] == ["linkedin"]:
            logging.getLogger("jobspy-test").error("DNS unavailable")
            return pd.DataFrame()
        return pd.DataFrame([{
            "id": "1",
            "site": "indeed",
            "job_url": "https://example.com/1",
            "title": "Junior Software Engineer",
            "company": "Example",
            "location": "Remote",
        }])

    store = JobStore(tmp_path / "jobs.sqlite3")
    summary = run_scrape_cycle(
        store,
        24,
        search_groups={"core_software": ["software engineer"]},
        sources=["indeed", "linkedin"],
        scraper=fake_scraper,
        query_delay_seconds=0,
        source_timeout_seconds=0,
        export_dir=tmp_path / "exports",
        process_queues=False,
    )

    assert len(summary.new_job_ids) == 1
    assert len(summary.errors) == 1
    assert "linkedin/core_software/software engineer: DNS unavailable" in summary.errors[0]
    assert store.get_run(summary.run_id)["status"] == "completed_with_errors"


def test_repeated_cycle_creates_no_duplicate_canonical_job(tmp_path):
    def fake_scraper(**kwargs):
        return pd.DataFrame(
            [
                {
                    "id": "1",
                    "site": "indeed",
                    "job_url": "https://example.com/1",
                    "title": "Associate Software Developer",
                    "company": "Example",
                    "location": "Boston, MA",
                }
            ]
        )

    store = JobStore(tmp_path / "jobs.sqlite3")
    options = {
        "search_groups": {"core_software": ["software developer"]},
        "sources": ["indeed"],
        "scraper": fake_scraper,
        "query_delay_seconds": 0,
        "source_timeout_seconds": 0,
        "export_dir": tmp_path / "exports",
    }
    first = run_scrape_cycle(store, 24, **options)
    second = run_scrape_cycle(store, 2, **options)

    assert len(first.new_job_ids) == 1
    assert len(second.new_job_ids) == 0
    assert second.duplicate_count == 1


def test_cli_status_list_and_export(tmp_path, capsys):
    database = tmp_path / "jobs.sqlite3"
    exports = tmp_path / "exports"
    store = JobStore(database)
    job_id, _ = store.upsert_job(
        {
            "id": "1",
            "site": "indeed",
            "job_url": "https://example.com/1",
            "title": "Software Engineer I",
            "company": "Example",
            "location": "Remote",
            "role_family": "core_software",
            "seniority": "unspecified",
            "matched_terms": ["software engineer"],
        },
        query_group="core_software",
        search_term="software engineer",
    )

    base = ["--database", str(database), "--output-dir", str(exports)]
    assert main([*base, "status", str(job_id), "saved", "--note", "Strong fit"]) == 0
    assert main([*base, "list", "--status", "saved"]) == 0
    assert main([*base, "export"]) == 0
    output = capsys.readouterr().out
    assert "marked saved" in output
    assert "Software Engineer I" in output
    assert (exports / "current-jobs.csv").exists()


def slow_scraper(**kwargs):
    time.sleep(2)
    return pd.DataFrame()


def fast_scraper(**kwargs):
    return pd.DataFrame()


def test_source_timeout_terminates_stalled_worker():
    with pytest.raises(TimeoutError, match="exceeded"):
        _scrape_source_with_timeout(
            "indeed", "software engineer", 24, slow_scraper, 0.05
        )


def test_indeed_runs_concurrently_while_linkedin_stays_serial(tmp_path, monkeypatch):
    active = {"indeed": 0, "linkedin": 0}
    maximum = {"indeed": 0, "linkedin": 0}
    lock = threading.Lock()

    def fake_scraper(**kwargs):
        source = kwargs["site_name"][0]
        with lock:
            active[source] += 1
            maximum[source] = max(maximum[source], active[source])
        time.sleep(0.04)
        with lock:
            active[source] -= 1
        return pd.DataFrame()

    monkeypatch.setattr("job_scraper.config.INDEED_MAX_WORKERS", 3)
    store = JobStore(tmp_path / "jobs.sqlite3")
    run_scrape_cycle(
        store,
        24,
        search_groups={"core_software": ["one", "two", "three"]},
        sources=["indeed", "linkedin"],
        scraper=fake_scraper,
        query_delay_seconds=0,
        source_timeout_seconds=0,
        export_dir=tmp_path / "exports",
        process_queues=False,
    )

    assert maximum["indeed"] > 1
    assert maximum["linkedin"] == 1


def test_parallel_indeed_lane_works_with_process_timeouts(tmp_path, monkeypatch):
    monkeypatch.setattr("job_scraper.config.INDEED_MAX_WORKERS", 2)
    store = JobStore(tmp_path / "jobs.sqlite3")
    summary = run_scrape_cycle(
        store,
        24,
        search_groups={"core_software": ["one", "two", "three"]},
        sources=["indeed"],
        scraper=fast_scraper,
        query_delay_seconds=0,
        source_timeout_seconds=5,
        export_dir=tmp_path / "exports",
        process_queues=False,
    )

    assert summary.errors == ()
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM scrape_attempts WHERE run_id=?", (summary.run_id,)
        ).fetchone()[0] == 3


def test_default_searches_have_fifteen_representatives_across_all_families():
    from job_scraper import config

    assert len(config.SEARCH_GROUPS) == 9
    assert sum(map(len, config.SEARCH_GROUPS.values())) == 15
    assert set(config.SEARCH_GROUPS) == set(config.ROLE_TERMS)
    assert config.SOURCES == ["indeed", "linkedin", "glassdoor"]


def test_glassdoor_results_use_the_normal_pipeline(tmp_path):
    def fake_scraper(**kwargs):
        assert kwargs["site_name"] == ["glassdoor"]
        return pd.DataFrame([{
            "id": "glassdoor-1",
            "site": "glassdoor",
            "job_url": "https://example.com/glassdoor-1",
            "title": "Junior Software Engineer",
            "company": "Example",
            "location": "Remote",
        }])

    store = JobStore(tmp_path / "jobs.sqlite3")
    summary = run_scrape_cycle(
        store,
        24,
        search_groups={"core_software": ["software engineer"]},
        sources=["glassdoor"],
        scraper=fake_scraper,
        query_delay_seconds=0,
        source_timeout_seconds=0,
        export_dir=tmp_path / "exports",
        process_queues=False,
    )

    assert summary.errors == ()
    assert len(summary.new_job_ids) == 1
    assert store.get_job_details(summary.new_job_ids[0])["postings"][0]["source"] == "glassdoor"


def empty_cycle_options(tmp_path):
    return {
        "search_groups": {"core_software": ["software engineer"]},
        "sources": ["indeed"],
        "scraper": lambda **_kwargs: pd.DataFrame(),
        "query_delay_seconds": 0,
        "source_timeout_seconds": 0,
        "export_dir": tmp_path / "exports",
    }


def latest_run(store):
    with store.connect() as connection:
        return dict(connection.execute(
            "SELECT * FROM scrape_runs ORDER BY id DESC LIMIT 1"
        ).fetchone())


def test_enrichment_crash_exports_then_finalizes_failed(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    def fail_enrichment(*_args, **_kwargs):
        raise RuntimeError("worker crashed")

    monkeypatch.setattr(main_module, "process_enrichment", fail_enrichment)

    with pytest.raises(RuntimeError, match="exports were written"):
        run_scrape_cycle(store, 24, **empty_cycle_options(tmp_path))

    assert latest_run(store)["status"] == "failed"
    assert "enrichment: RuntimeError: worker crashed" in latest_run(store)["error_summary"]
    assert (tmp_path / "exports" / "current-jobs.csv").exists()


def test_enrichment_summary_crash_exports_then_finalizes_failed(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    def fail_summary(*_args, **_kwargs):
        raise RuntimeError("summary failed")

    monkeypatch.setattr(store, "record_enrichment_summary", fail_summary)

    with pytest.raises(RuntimeError, match="exports were written"):
        run_scrape_cycle(
            store, 24, process_queues=False, **empty_cycle_options(tmp_path)
        )

    assert latest_run(store)["status"] == "failed"
    assert (tmp_path / "exports" / "current-jobs.csv").exists()


def test_run_summary_counts_analysis_tasks_created_after_fetch(tmp_path, monkeypatch):
    stats = EnrichmentStats(fetch=WorkerStats(completed=2, analysis_created=2))
    monkeypatch.setattr(main_module, "process_enrichment", lambda *_args, **_kwargs: stats)
    store = JobStore(tmp_path / "jobs.sqlite3")

    summary = run_scrape_cycle(store, 24, **empty_cycle_options(tmp_path))

    assert summary.analysis_tasks_created == 2
    assert latest_run(store)["analysis_tasks_created"] == 2


def test_export_crash_finalizes_failed(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")
    monkeypatch.setattr(
        main_module, "export_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("export failed")),
    )

    with pytest.raises(RuntimeError, match="export failed"):
        run_scrape_cycle(
            store, 24, process_queues=False, **empty_cycle_options(tmp_path)
        )

    run = latest_run(store)
    assert run["status"] == "failed"
    assert "export: RuntimeError: export failed" in run["error_summary"]


def test_enrichment_interrupt_finalizes_interrupted(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    def interrupt(*_args, **_kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main_module, "process_enrichment", interrupt)

    with pytest.raises(KeyboardInterrupt):
        run_scrape_cycle(store, 24, **empty_cycle_options(tmp_path))

    assert latest_run(store)["status"] == "interrupted"


def test_queue_initialization_failure_finalizes_run(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    class BrokenQueue:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("queue failed")

    monkeypatch.setattr(main_module, "EnrichmentQueue", BrokenQueue)

    with pytest.raises(RuntimeError, match="queue failed"):
        run_scrape_cycle(store, 24, **empty_cycle_options(tmp_path))

    assert latest_run(store)["status"] == "failed"


def test_enrich_limit_must_be_positive():
    with pytest.raises(SystemExit) as error:
        main(["enrich", "--limit", "0"])
    assert error.value.code == 2


def test_show_reports_source_task_status_and_error(tmp_path, capsys):
    database = tmp_path / "jobs.sqlite3"
    store = JobStore(database)
    job_id, _ = store.upsert_job(
        {
            "id": "linkedin-show",
            "site": "linkedin",
            "job_url": "https://www.linkedin.com/jobs/view/show",
            "title": "Software Engineer I",
            "company": "Example",
            "location": "Remote",
            "role_family": "core_software",
            "seniority": "entry",
            "matched_terms": ["software engineer"],
        },
        query_group="core_software",
        search_term="software engineer",
    )
    queue = EnrichmentQueue(store)
    queue.sync_job(job_id)
    task = queue.claim("fetch_description", 1)[0]
    queue.fail(task["id"], task["lease_token"], FetchError("gone"), permanent=True)

    assert main(["--database", str(database), "show", str(job_id)]) == 0
    output = capsys.readouterr().out
    assert "Analysis: dead" in output
    assert "fetch_description status=dead attempts=1" in output
    assert "Last error: FetchError: gone" in output


def test_run_command_returns_clean_nonzero_for_cycle_failure(monkeypatch, capsys):
    def fail_cycle(*_args, **_kwargs):
        raise RuntimeError("cycle failed")

    monkeypatch.setattr(main_module, "run_scrape_cycle", fail_cycle)

    assert main(["run", "--once"]) == 1
    assert "Scrape failed: cycle failed" in capsys.readouterr().err


def test_run_once_returns_nonzero_for_partial_source_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(
        main_module,
        "run_scrape_cycle",
        lambda *_args, **_kwargs: SimpleNamespace(errors=("linkedin failed",)),
    )
    monkeypatch.setattr(main_module, "_print_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "_email_new_jobs", lambda *_args: None)

    assert main([
        "--database", str(tmp_path / "jobs.sqlite3"),
        "run", "--once", "--no-enrichment",
    ]) == 1


def test_run_no_enrichment_skips_queue_processing(tmp_path, monkeypatch):
    captured = {}

    def fake_cycle(*_args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(errors=(), new_export=None)

    monkeypatch.setattr(main_module, "run_scrape_cycle", fake_cycle)
    monkeypatch.setattr(main_module, "_print_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "_email_new_jobs", lambda *_args: None)

    assert main([
        "--database", str(tmp_path / "jobs.sqlite3"),
        "run", "--once", "--no-enrichment",
    ]) == 0
    assert captured["process_queues"] is False
