import sqlite3

import pytest

from job_scraper.storage import APPLICATION_STATUSES, JobStore, normalize_url


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


def test_application_is_preserved_during_later_upsert(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    store.create_application(job_id, "Applied through company site")
    store.set_application_status(job_id, "interviewing", "Phone screen scheduled")
    store.upsert_job(
        job_row(description="Updated description"),
        query_group="core_software",
        search_term="software engineer",
    )

    with store.connect() as connection:
        job = connection.execute(
            "SELECT id FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        application = connection.execute(
            "SELECT status, notes, applied_at, interviewing_at FROM applications WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        history = connection.execute(
            "SELECT old_status, new_status FROM application_events "
            "WHERE application_id = (SELECT id FROM applications WHERE job_id = ?)",
            (job_id,),
        ).fetchall()
    assert job["id"] == job_id
    assert tuple(application[:2]) == ("interviewing", "Phone screen scheduled")
    assert application["applied_at"]
    assert application["interviewing_at"]
    assert [tuple(row) for row in history] == [(None, "applied"), ("applied", "interviewing")]


def test_application_creation_is_idempotent(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    first, created = store.create_application(job_id, "First application note")
    second, created_again = store.create_application(job_id, "A duplicate note")

    assert created is True
    assert created_again is False
    assert first["id"] == second["id"]
    assert second["notes"] == "First application note"
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM applications WHERE job_id = ?", (job_id,)
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM application_events WHERE application_id = ?",
            (first["id"],),
        ).fetchone()[0] == 1


def test_application_status_validation_and_missing_records(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    with pytest.raises(ValueError):
        store.set_application_status(1, "not-a-stage")
    with pytest.raises(KeyError):
        store.set_application_status(999, "interviewing")


def test_application_dates_are_first_entry_timestamps(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    store.create_application(job_id)
    store.set_application_status(job_id, "interviewing")
    first_interviewing_at = store.get_application_details(job_id)["application"]["interviewing_at"]
    store.set_application_status(job_id, "offer")
    store.set_application_status(job_id, "interviewing", "Re-entered interview stage")
    application = store.get_application_details(job_id)["application"]
    assert application["interviewing_at"] == first_interviewing_at


def test_all_application_stages_are_supported(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id, _ = store.upsert_job(
        job_row(), query_group="core_software", search_term="software engineer"
    )
    store.create_application(job_id)
    for stage in APPLICATION_STATUSES[1:]:
        application = store.set_application_status(job_id, stage)
        assert application["status"] == stage
        assert application[f"{stage}_at"]


def test_new_schema_does_not_retain_legacy_job_workflow_columns(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    with store.connect() as connection:
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(jobs)")
        }
        tables = {
            row["name"] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert not {"status", "notes", "applied_at"}.intersection(columns)
    assert "status_history" not in tables
    assert {"applications", "application_events"}.issubset(tables)


def test_legacy_applied_job_is_backfilled_during_schema_migration(tmp_path):
    database = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                normalized_title TEXT NOT NULL,
                company TEXT,
                normalized_company TEXT NOT NULL,
                location TEXT,
                normalized_location TEXT NOT NULL,
                is_remote INTEGER,
                role_family TEXT NOT NULL,
                seniority TEXT NOT NULL,
                eligible INTEGER NOT NULL DEFAULT 1,
                eligibility_reason TEXT NOT NULL DEFAULT 'accepted',
                status TEXT NOT NULL,
                notes TEXT NOT NULL DEFAULT '',
                date_posted TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                applied_at TEXT
            );
            CREATE TABLE status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                note TEXT,
                changed_at TEXT NOT NULL
            );
            INSERT INTO jobs(
                fingerprint, title, normalized_title, company, normalized_company,
                location, normalized_location, role_family, seniority, status, notes,
                first_seen_at, last_seen_at, updated_at, applied_at
            ) VALUES (
                'legacy-fingerprint', 'Software Engineer', 'software engineer',
                'Example', 'example', 'Remote', 'remote', 'core_software', 'entry',
                'applied', 'Legacy application', '2026-09-26T00:00:00+00:00',
                '2026-09-26T00:00:00+00:00', '2026-09-26T00:00:00+00:00',
                '2026-09-26T01:00:00+00:00'
            );
            """
        )

    store = JobStore(database)
    application = store.get_application_details(1)["application"]
    assert application["status"] == "applied"
    assert application["notes"] == "Legacy application"
    assert application["applied_at"] == "2026-09-26T01:00:00+00:00"
    with store.connect() as connection:
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(jobs)")}
    assert not {"status", "notes", "applied_at"}.intersection(columns)
    assert (database.with_name("legacy.sqlite3.pre-application-migration.bak")).exists()


def test_application_pipeline_rows_include_only_tracked_jobs(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    applied_id, _ = store.upsert_job(
        job_row(id="applied"), query_group="core_software", search_term="software engineer"
    )
    untracked_id, _ = store.upsert_job(
        job_row(
            id="untracked",
            job_url="https://example.com/untracked",
            title="Different Software Engineer",
        ),
        query_group="core_software", search_term="software engineer"
    )
    store.create_application(applied_id)
    rows = store.export_application_rows()
    assert [row["id"] for row in rows] == [applied_id]
    assert untracked_id not in [row["id"] for row in rows]


def test_schema_version_is_recorded(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    with sqlite3.connect(store.path) as connection:
        version = connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
    assert version == "4"


def test_application_creation_validates_missing_jobs(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    with pytest.raises(ValueError):
        store.set_application_status(1, "not-a-stage")
    with pytest.raises(KeyError):
        store.create_application(999)


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
