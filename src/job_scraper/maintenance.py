"""One-time maintenance operations for the SQLite history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sqlite3


@dataclass(frozen=True)
class PurgeSummary:
    """Counts from removing postings for one source."""

    source_postings: int
    source_attempts: int
    deleted_jobs: int
    preserved_jobs: int


def backup_database(
    database: str | Path,
    backup_directory: str | Path,
    *,
    label: str = "source-purge",
) -> Path:
    """Copy a database to a timestamped backup path before destructive work."""
    source = Path(database)
    if not source.is_file():
        raise FileNotFoundError(source)
    directory = Path(backup_directory)
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = directory / f"{source.name}.pre-{label}-{timestamp}.bak"
    shutil.copy2(source, target)
    return target


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        row[1] for row in connection.execute(f"PRAGMA table_info({table})")
    }


def _delete_ids(
    connection: sqlite3.Connection,
    table: str,
    column: str,
    ids: set[int],
) -> int:
    if not ids or table not in _tables(connection):
        return 0
    placeholders = ",".join("?" for _ in ids)
    cursor = connection.execute(
        f"DELETE FROM {table} WHERE {column} IN ({placeholders})",
        sorted(ids),
    )
    return cursor.rowcount


def purge_source_data(database: str | Path, source: str) -> PurgeSummary:
    """Remove one source while preserving canonical jobs with other sources.

    The operation is deliberately generic so it can clean databases created by
    either the legacy job-status workflow or the application-tracking schema.
    It does not alter scrape runs or unrelated source history.
    """
    source = source.casefold()
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        tables = _tables(connection)
        posting_rows = connection.execute(
            "SELECT id, job_id FROM postings WHERE source = ?", (source,)
        ).fetchall()
        posting_ids = {int(row[0]) for row in posting_rows}
        source_job_ids = {int(row[1]) for row in posting_rows}
        if source_job_ids:
            mixed_job_ids = {
                int(row[0])
                for row in connection.execute(
                    """
                    SELECT DISTINCT p.job_id
                    FROM postings p
                    WHERE p.job_id IN ({jobs})
                      AND p.source != ?
                    """.format(jobs=",".join("?" for _ in source_job_ids)),
                    [*sorted(source_job_ids), source],
                ).fetchall()
            }
        else:
            mixed_job_ids = set()
        source_only_job_ids = source_job_ids - mixed_job_ids

        source_attempts = 0
        if "scrape_attempts" in tables:
            source_attempts = connection.execute(
                "DELETE FROM scrape_attempts WHERE source = ?", (source,)
            ).rowcount

        task_ids: set[int] = set()
        if "enrichment_tasks" in tables and posting_ids:
            task_ids = {
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM enrichment_tasks WHERE posting_id IN ({})".format(
                        ",".join("?" for _ in posting_ids)
                    ),
                    sorted(posting_ids),
                ).fetchall()
            }
        _delete_ids(connection, "llm_usage", "task_id", task_ids)
        _delete_ids(connection, "enrichment_task_events", "task_id", task_ids)
        _delete_ids(connection, "enrichment_tasks", "id", task_ids)

        analysis_ids: set[int] = set()
        if "posting_analyses" in tables and posting_ids:
            analysis_ids = {
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM posting_analyses WHERE posting_id IN ({})".format(
                        ",".join("?" for _ in posting_ids)
                    ),
                    sorted(posting_ids),
                ).fetchall()
            }
        _delete_ids(connection, "posting_requirements", "analysis_id", analysis_ids)
        _delete_ids(connection, "posting_requirements", "posting_id", posting_ids)
        _delete_ids(connection, "posting_analyses", "id", analysis_ids)
        _delete_ids(connection, "postings", "id", posting_ids)

        if source_only_job_ids:
            application_ids: set[int] = set()
            if "applications" in tables:
                application_columns = _columns(connection, "applications")
                if "job_id" in application_columns:
                    application_ids = {
                        int(row[0])
                        for row in connection.execute(
                            "SELECT id FROM applications WHERE job_id IN ({})".format(
                                ",".join("?" for _ in source_only_job_ids)
                            ),
                            sorted(source_only_job_ids),
                        ).fetchall()
                    }
            if "application_events" in tables:
                event_columns = _columns(connection, "application_events")
                if "application_id" in event_columns:
                    _delete_ids(connection, "application_events", "application_id", application_ids)
                elif "job_id" in event_columns:
                    _delete_ids(connection, "application_events", "job_id", source_only_job_ids)
            _delete_ids(connection, "applications", "id", application_ids)
            _delete_ids(connection, "job_matches", "job_id", source_only_job_ids)
            _delete_ids(connection, "status_history", "job_id", source_only_job_ids)
            _delete_ids(connection, "jobs", "id", source_only_job_ids)

        connection.commit()
        return PurgeSummary(
            source_postings=len(posting_ids),
            source_attempts=source_attempts,
            deleted_jobs=len(source_only_job_ids),
            preserved_jobs=len(mixed_job_ids),
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
