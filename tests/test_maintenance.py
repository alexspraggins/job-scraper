import sqlite3
from contextlib import closing

from job_scraper.maintenance import backup_database, purge_source_data


def seed_source_history(database):
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE jobs (id INTEGER PRIMARY KEY);
            CREATE TABLE postings (id INTEGER PRIMARY KEY, job_id INTEGER, source TEXT);
            CREATE TABLE scrape_attempts (id INTEGER PRIMARY KEY, source TEXT);
            CREATE TABLE enrichment_tasks (id INTEGER PRIMARY KEY, posting_id INTEGER);
            CREATE TABLE enrichment_task_events (id INTEGER PRIMARY KEY, task_id INTEGER);
            CREATE TABLE llm_usage (id INTEGER PRIMARY KEY, task_id INTEGER);
            CREATE TABLE posting_analyses (id INTEGER PRIMARY KEY, posting_id INTEGER);
            CREATE TABLE posting_requirements (id INTEGER PRIMARY KEY, analysis_id INTEGER, posting_id INTEGER);
            CREATE TABLE job_matches (id INTEGER PRIMARY KEY, job_id INTEGER);
            CREATE TABLE status_history (id INTEGER PRIMARY KEY, job_id INTEGER);
            CREATE TABLE applications (id INTEGER PRIMARY KEY, job_id INTEGER);
            CREATE TABLE application_events (id INTEGER PRIMARY KEY, application_id INTEGER);
            INSERT INTO jobs VALUES (1), (2);
            INSERT INTO postings VALUES (11, 1, 'glassdoor'), (21, 2, 'glassdoor'), (22, 2, 'indeed');
            INSERT INTO scrape_attempts VALUES (1, 'glassdoor'), (2, 'indeed');
            INSERT INTO enrichment_tasks VALUES (101, 11);
            INSERT INTO enrichment_task_events VALUES (201, 101);
            INSERT INTO llm_usage VALUES (301, 101);
            INSERT INTO posting_analyses VALUES (401, 11);
            INSERT INTO posting_requirements VALUES (501, 401, 11);
            INSERT INTO job_matches VALUES (601, 1);
            INSERT INTO status_history VALUES (701, 1);
            INSERT INTO applications VALUES (801, 1);
            INSERT INTO application_events VALUES (901, 801);
            """
        )
        connection.commit()


def test_backup_and_source_purge_preserve_mixed_source_jobs(tmp_path):
    database = tmp_path / "jobs.sqlite3"
    seed_source_history(database)

    backup = backup_database(database, tmp_path / "backups")
    summary = purge_source_data(database, "GlassDoor")

    assert backup.is_file()
    assert summary.source_postings == 2
    assert summary.source_attempts == 1
    assert summary.deleted_jobs == 1
    assert summary.preserved_jobs == 1
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT id FROM jobs ORDER BY id").fetchall() == [(2,)]
        assert connection.execute("SELECT source FROM postings").fetchall() == [("indeed",)]
        assert connection.execute("SELECT source FROM scrape_attempts").fetchall() == [("indeed",)]
        for table in (
            "enrichment_tasks", "enrichment_task_events", "llm_usage",
            "posting_analyses", "posting_requirements", "applications",
            "application_events", "job_matches", "status_history",
        ):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
