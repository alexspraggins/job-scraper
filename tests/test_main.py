import pandas as pd
import time
import pytest

from job_scraper.main import _scrape_source_with_timeout, main, run_scrape_cycle
from job_scraper.storage import JobStore


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


def test_repeated_cycle_creates_no_duplicate_canonical_job(tmp_path):
    def fake_scraper(**kwargs):
        return pd.DataFrame(
            [
                {
                    "id": "1",
                    "site": "indeed",
                    "job_url": "https://example.com/1",
                    "title": "Software Developer",
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
            "title": "Software Engineer",
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
    assert "Software Engineer" in output
    assert (exports / "current_jobs.csv").exists()


def slow_scraper(**kwargs):
    time.sleep(2)
    return pd.DataFrame()


def test_source_timeout_terminates_stalled_worker():
    with pytest.raises(TimeoutError, match="exceeded"):
        _scrape_source_with_timeout(
            "indeed", "software engineer", 24, slow_scraper, 0.05
        )
