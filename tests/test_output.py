import csv
import re

import pytest

from job_scraper import config
from job_scraper.output import EXPORT_COLUMNS, export_all, write_csv
from job_scraper.queueing import EnrichmentQueue
from job_scraper.storage import JobStore


def test_exports_current_and_per_run_csv_without_separator_rows(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        {
            "id": "1",
            "site": "indeed",
            "job_url": "https://example.com/1",
            "title": "Junior Backend Engineer",
            "company": "Example",
            "location": "Remote",
            "is_remote": True,
            "role_family": "backend_full_stack",
            "seniority": "unspecified",
            "matched_terms": ["backend engineer"],
            "min_amount": 90000,
            "max_amount": 110000,
            "currency": "USD",
            "interval": "yearly",
        },
        query_group="backend_full_stack",
        search_term="backend engineer",
    )

    current_path, run_path = export_all(store, tmp_path / "exports", [job_id])

    assert current_path.exists()
    assert run_path and run_path.exists()
    assert current_path.name == "current-jobs.csv"
    assert re.fullmatch(r"new-jobs-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.csv", run_path.name)
    with current_path.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    assert list(rows[0]) == EXPORT_COLUMNS
    assert len(rows) == 1
    assert rows[0]["title"] == "Junior Backend Engineer"
    assert rows[0]["compensation"] == "USD 90000-110000/yearly"
    assert "Jobs Scraped at" not in current_path.read_text()


def test_empty_new_job_ids_do_not_create_run_export(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    current_path, run_path = export_all(store, tmp_path / "exports", [])
    assert current_path.exists()
    assert run_path is None


def add_posting(store, *, source, source_id, description=None):
    return store.upsert_job(
        {
            "id": source_id,
            "site": source,
            "job_url": f"https://example.com/{source}/{source_id}",
            "title": "Software Engineer I",
            "company": "Shared Company",
            "location": "Remote",
            "description": description,
            "role_family": "core_software",
            "seniority": "entry",
            "matched_terms": ["software engineer"],
        },
        query_group="core_software",
        search_term="software engineer",
    )[0]


def complete_analysis(queue):
    task = queue.claim("analyze_description", 1)[0]
    queue.complete_analysis(task["id"], task["lease_token"], [])
    return task["id"]


def test_analysis_status_uses_worst_posting_state(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(
        store, source="indeed", source_id="complete", description="Python required"
    )
    queue.sync_job(job_id)
    complete_analysis(queue)
    add_posting(
        store, source="linkedin", source_id="pending", description="SQL required"
    )
    queue.sync_job(job_id)

    assert store.export_rows()[0]["analysis_status"] == "pending"

    add_posting(store, source="skillsire", source_id="unavailable")
    assert store.export_rows()[0]["analysis_status"] == "unavailable"

    task = queue.claim("analyze_description", 1)[0]
    queue.fail(task["id"], task["lease_token"], RuntimeError("failed"), permanent=True)
    assert store.export_rows()[0]["analysis_status"] == "dead"


def test_current_analysis_result_is_complete_without_task_row(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(
        store, source="indeed", source_id="result", description="Python required"
    )
    queue.sync_job(job_id)
    task_id = complete_analysis(queue)
    with store.connect() as connection:
        connection.execute("DELETE FROM enrichment_tasks WHERE id=?", (task_id,))

    assert store.export_rows()[0]["analysis_status"] == "completed"
    assert queue.repair_orphans(fetch_limit=1, analysis_limit=1)["analysis_created"] == 0


def test_stale_model_analysis_does_not_count_as_complete(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(
        store, source="indeed", source_id="stale", description="Python required"
    )
    queue.sync_job(job_id)
    complete_analysis(queue)
    monkeypatch.setattr(config, "LLM_MODEL", "new-model")

    assert store.export_rows()[0]["analysis_status"] == "not_queued"


def test_atomic_csv_failure_preserves_previous_file(tmp_path, monkeypatch):
    target = tmp_path / "current-jobs.csv"
    target.write_text("previous contents\n", encoding="utf-8")

    def fail_rows(_self, _rows):
        raise RuntimeError("disk failed")

    monkeypatch.setattr(csv.DictWriter, "writerows", fail_rows)

    with pytest.raises(RuntimeError, match="disk failed"):
        write_csv(target, [{"id": 1}])

    assert target.read_text(encoding="utf-8") == "previous contents\n"
    assert list(tmp_path.glob(".current-jobs.csv.*.tmp")) == []
