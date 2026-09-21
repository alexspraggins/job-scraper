import csv

from job_scraper.output import EXPORT_COLUMNS, export_all
from job_scraper.storage import JobStore


def test_exports_current_and_per_run_csv_without_separator_rows(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        {
            "id": "1",
            "site": "indeed",
            "job_url": "https://example.com/1",
            "title": "Backend Engineer",
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
    with current_path.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    assert list(rows[0]) == EXPORT_COLUMNS
    assert len(rows) == 1
    assert rows[0]["title"] == "Backend Engineer"
    assert rows[0]["compensation"] == "USD 90000-110000/yearly"
    assert "Jobs Scraped at" not in current_path.read_text()


def test_empty_new_job_ids_do_not_create_run_export(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    current_path, run_path = export_all(store, tmp_path / "exports", [])
    assert current_path.exists()
    assert run_path is None
