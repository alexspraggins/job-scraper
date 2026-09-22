import sqlite3

import pytest

from job_scraper.storage import JobStore, normalize_url


def job_row(**overrides):
    row = {
        "id": "source-1",
        "site": "indeed",
        "job_url": "https://example.com/job/1?utm_source=test",
        "job_url_direct": "https://company.example/jobs/1",
        "title": "Software Engineer",
        "company": "Example Corp",
        "location": "Remote",
        "is_remote": True,
        "role_family": "core_software",
        "seniority": "unspecified",
        "matched_terms": ["software engineer"],
        "date_posted": "2026-09-21",
    }
    row.update(overrides)
    return row


def test_url_normalization_removes_tracking_but_keeps_job_keys():
    url = normalize_url("https://Indeed.com/viewjob?utm_source=x&jk=abc#top")
    assert url == "https://indeed.com/viewjob?jk=abc"


def test_upsert_deduplicates_postings_and_cross_source_jobs(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    first_id, first_new = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    repeated_id, repeated_new = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    linkedin_id, linkedin_new = store.upsert_job(
        job_row(
            id="linkedin-1",
            site="linkedin",
            job_url="https://linkedin.example/jobs/1",
            job_url_direct="https://company.example/jobs/1",
        ),
        query_group="core_software",
        search_term="software developer",
    )

    assert first_new
    assert not repeated_new
    assert not linkedin_new
    assert first_id == repeated_id == linkedin_id
    with store.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM postings").fetchone()[0] == 2


def test_status_is_preserved_during_later_upsert(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    store.set_status(job_id, "applied", "Applied through company site")
    store.upsert_job(
        job_row(description="Updated description"),
        query_group="core_software",
        search_term="software engineer",
    )

    with store.connect() as connection:
        job = connection.execute(
            "SELECT status, notes, applied_at FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        history = connection.execute(
            "SELECT old_status, new_status FROM status_history WHERE job_id = ?", (job_id,)
        ).fetchone()
    assert tuple(job[:2]) == ("applied", "Applied through company site")
    assert job["applied_at"]
    assert tuple(history) == ("new", "applied")


def test_schema_version_is_recorded(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    with sqlite3.connect(store.path) as connection:
        version = connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
    assert version == "3"


def test_status_validation_and_missing_job_errors(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    with pytest.raises(ValueError):
        store.set_status(1, "interviewing")
    with pytest.raises(KeyError):
        store.set_status(999, "reviewed")


def test_reclassification_hides_ineligible_jobs_without_deleting_them(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    eligible_id, _ = store.upsert_job(
        job_row(id="entry", title="Software Engineer I", job_url="https://example.com/entry"),
        query_group="core_software",
        search_term="software engineer",
    )
    ineligible_id, _ = store.upsert_job(
        job_row(id="level-2", title="Software Engineer II", job_url="https://example.com/level-2"),
        query_group="core_software",
        search_term="software engineer",
    )

    eligible, ineligible = store.reclassify_jobs()
    exported_ids = [row["id"] for row in store.export_rows()]

    assert (eligible, ineligible) == (1, 1)
    assert exported_ids == [eligible_id]
    with store.connect() as connection:
        stored = connection.execute(
            "SELECT eligible, eligibility_reason FROM jobs WHERE id = ?",
            (ineligible_id,),
        ).fetchone()
    assert tuple(stored) == (0, "excluded:level 2+")
