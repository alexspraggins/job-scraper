"""SQLite persistence, deduplication, and workflow status management."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from . import config
from .filtering import classify_title, normalize_text


SCHEMA_VERSION = 3
VALID_STATUSES = ("new", "reviewed", "saved", "applied", "rejected")
ANALYSIS_STATUS_PRIORITY = {
    "dead": 0,
    "unavailable": 1,
    "not_queued": 2,
    "budget_blocked": 3,
    "retry": 4,
    "pending": 5,
    "completed": 6,
}
TRACKING_QUERY_KEYS = {
    "ref",
    "refid",
    "source",
    "trk",
    "trackingid",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_value(value: object) -> object | None:
    if value is None:
        return None
    try:
        if value != value:  # NaN and pandas NA-like values
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item") and callable(value.item):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    return value


def _as_utc(value: datetime) -> datetime:
    """Normalize a timestamp for comparisons with the UTC database clock."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_precise_timestamp(value: object) -> datetime | None:
    """Parse timestamps while rejecting date-only values as imprecise."""
    cleaned = clean_value(value)
    if cleaned is None:
        return None
    if isinstance(cleaned, datetime):
        return _as_utc(cleaned)
    text = str(cleaned).strip()
    if not text or re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _is_current_job(row: dict, cutoff: datetime) -> bool:
    """Return whether a job belongs in the rolling current-jobs view."""
    posted_at = _parse_precise_timestamp(row.get("date_posted"))
    first_seen_at = _parse_precise_timestamp(row.get("first_seen_at"))
    effective_at = posted_at or first_seen_at
    return effective_at is not None and effective_at >= cutoff


def normalize_url(value: object) -> str:
    url = str(clean_value(value) or "").strip()
    if not url:
        return ""
    parts = urlsplit(url)
    filtered_query = []
    for key, item in parse_qsl(parts.query, keep_blank_values=True):
        lower_key = key.casefold()
        if lower_key.startswith("utm_") or lower_key in TRACKING_QUERY_KEYS:
            continue
        filtered_query.append((key, item))
    path = re.sub(r"/+$", "", parts.path) or "/"
    return urlunsplit(
        (
            parts.scheme.casefold(),
            parts.netloc.casefold(),
            path,
            urlencode(sorted(filtered_query)),
            "",
        )
    )


def job_fingerprint(row: dict) -> str:
    title = normalize_text(row.get("title"))
    company = normalize_text(row.get("company") or row.get("company_name"))
    location = normalize_text(row.get("location"))
    if title and company:
        material = f"{title}|{company}|{location}"
    else:
        material = normalize_url(row.get("job_url_direct") or row.get("job_url"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class JobStore:
    def __init__(self, database_path: str | Path):
        self.path = Path(database_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS jobs (
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
                    status TEXT NOT NULL DEFAULT 'new'
                        CHECK (status IN ('new', 'reviewed', 'saved', 'applied', 'rejected')),
                    notes TEXT NOT NULL DEFAULT '',
                    date_posted TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    applied_at TEXT
                );

                CREATE TABLE IF NOT EXISTS postings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    source_job_id TEXT,
                    job_url TEXT,
                    direct_url TEXT,
                    normalized_url TEXT,
                    description TEXT,
                    job_type TEXT,
                    salary_interval TEXT,
                    min_amount REAL,
                    max_amount REAL,
                    currency TEXT,
                    query_group TEXT NOT NULL,
                    search_term TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    UNIQUE (source, source_key)
                );

                CREATE TABLE IF NOT EXISTS job_matches (
                    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    role_family TEXT NOT NULL,
                    matched_term TEXT NOT NULL,
                    query_group TEXT NOT NULL,
                    PRIMARY KEY (job_id, role_family, matched_term, query_group)
                );

                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    lookback_hours INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    raw_count INTEGER NOT NULL DEFAULT 0,
                    accepted_count INTEGER NOT NULL DEFAULT 0,
                    excluded_count INTEGER NOT NULL DEFAULT 0,
                    unmatched_count INTEGER NOT NULL DEFAULT 0,
                    duplicate_count INTEGER NOT NULL DEFAULT 0,
                    new_count INTEGER NOT NULL DEFAULT 0,
                    error_summary TEXT,
                    description_tasks_created INTEGER NOT NULL DEFAULT 0,
                    description_tasks_completed INTEGER NOT NULL DEFAULT 0,
                    description_tasks_retried INTEGER NOT NULL DEFAULT 0,
                    description_tasks_dead INTEGER NOT NULL DEFAULT 0,
                    analysis_tasks_created INTEGER NOT NULL DEFAULT 0,
                    analysis_tasks_completed INTEGER NOT NULL DEFAULT 0,
                    analysis_tasks_retried INTEGER NOT NULL DEFAULT 0,
                    analysis_tasks_budget_blocked INTEGER NOT NULL DEFAULT 0,
                    analysis_tasks_dead INTEGER NOT NULL DEFAULT 0,
                    openai_calls INTEGER NOT NULL DEFAULT 0,
                    openai_input_tokens INTEGER NOT NULL DEFAULT 0,
                    openai_output_tokens INTEGER NOT NULL DEFAULT 0,
                    openai_estimated_cost REAL NOT NULL DEFAULT 0,
                    enrichment_error_summary TEXT,
                    circuit_breaker_state TEXT
                );

                CREATE TABLE IF NOT EXISTS scrape_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES scrape_runs(id) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    query_group TEXT NOT NULL,
                    search_term TEXT NOT NULL,
                    raw_count INTEGER NOT NULL DEFAULT 0,
                    accepted_count INTEGER NOT NULL DEFAULT 0,
                    excluded_count INTEGER NOT NULL DEFAULT 0,
                    unmatched_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    old_status TEXT,
                    new_status TEXT NOT NULL,
                    note TEXT,
                    changed_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_postings_job ON postings(job_id);
                CREATE INDEX IF NOT EXISTS idx_postings_url ON postings(normalized_url);
                CREATE INDEX IF NOT EXISTS idx_attempts_run ON scrape_attempts(run_id);
                """
            )
            job_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(jobs)")
            }
            if "eligible" not in job_columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN eligible INTEGER NOT NULL DEFAULT 1"
                )
            if "eligibility_reason" not in job_columns:
                connection.execute(
                    """
                    ALTER TABLE jobs ADD COLUMN eligibility_reason TEXT
                    NOT NULL DEFAULT 'accepted'
                    """
                )
            run_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(scrape_runs)")
            }
            for name, definition in (
                ("description_tasks_created", "INTEGER NOT NULL DEFAULT 0"),
                ("description_tasks_completed", "INTEGER NOT NULL DEFAULT 0"),
                ("description_tasks_retried", "INTEGER NOT NULL DEFAULT 0"),
                ("description_tasks_dead", "INTEGER NOT NULL DEFAULT 0"),
                ("analysis_tasks_created", "INTEGER NOT NULL DEFAULT 0"),
                ("analysis_tasks_completed", "INTEGER NOT NULL DEFAULT 0"),
                ("analysis_tasks_retried", "INTEGER NOT NULL DEFAULT 0"),
                ("analysis_tasks_budget_blocked", "INTEGER NOT NULL DEFAULT 0"),
                ("analysis_tasks_dead", "INTEGER NOT NULL DEFAULT 0"),
                ("openai_calls", "INTEGER NOT NULL DEFAULT 0"),
                ("openai_input_tokens", "INTEGER NOT NULL DEFAULT 0"),
                ("openai_output_tokens", "INTEGER NOT NULL DEFAULT 0"),
                ("openai_estimated_cost", "REAL NOT NULL DEFAULT 0"),
                ("enrichment_error_summary", "TEXT"),
                ("circuit_breaker_state", "TEXT"),
            ):
                if name not in run_columns:
                    connection.execute(f"ALTER TABLE scrape_runs ADD COLUMN {name} {definition}")
            from .queueing import initialize_enrichment_schema
            initialize_enrichment_schema(connection)
            connection.execute(
                """
                INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(SCHEMA_VERSION),),
            )
            connection.execute(
                """
                UPDATE scrape_runs
                SET status = 'interrupted', finished_at = ?,
                    error_summary = COALESCE(error_summary, 'Process ended before run completion')
                WHERE status = 'running'
                """,
                (utc_now(),),
            )

    def start_run(self, lookback_hours: int) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO scrape_runs(started_at, lookback_hours, status) VALUES (?, ?, 'running')",
                (utc_now(), lookback_hours),
            )
            return int(cursor.lastrowid)

    def record_attempt(
        self,
        run_id: int,
        source: str,
        query_group: str,
        search_term: str,
        raw_count: int = 0,
        accepted_count: int = 0,
        excluded_count: int = 0,
        unmatched_count: int = 0,
        error: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO scrape_attempts(
                    run_id, source, query_group, search_term, raw_count,
                    accepted_count, excluded_count, unmatched_count, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source,
                    query_group,
                    search_term,
                    raw_count,
                    accepted_count,
                    excluded_count,
                    unmatched_count,
                    error,
                ),
            )

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        raw_count: int,
        accepted_count: int,
        excluded_count: int,
        unmatched_count: int,
        duplicate_count: int,
        new_count: int,
        errors: list[str],
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scrape_runs
                SET finished_at = ?, status = ?, raw_count = ?, accepted_count = ?,
                    excluded_count = ?, unmatched_count = ?, duplicate_count = ?,
                    new_count = ?, error_summary = ?
                WHERE id = ?
                """,
                (
                    utc_now(),
                    status,
                    raw_count,
                    accepted_count,
                    excluded_count,
                    unmatched_count,
                    duplicate_count,
                    new_count,
                    "\n".join(errors) or None,
                    run_id,
                ),
            )

    def record_enrichment_summary(
        self, run_id: int, *, fetch_created: int, fetch_completed: int,
        fetch_retried: int, fetch_dead: int, analysis_created: int,
        analysis_completed: int, analysis_retried: int, analysis_blocked: int,
        analysis_dead: int, calls: int, input_tokens: int, output_tokens: int,
        estimated_cost: float, errors: list[str], circuit_breakers: list[str],
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """UPDATE scrape_runs SET description_tasks_created=?,
                   description_tasks_completed=?, description_tasks_retried=?,
                   description_tasks_dead=?, analysis_tasks_created=?,
                   analysis_tasks_completed=?, analysis_tasks_retried=?,
                   analysis_tasks_budget_blocked=?, analysis_tasks_dead=?,
                   openai_calls=?, openai_input_tokens=?, openai_output_tokens=?,
                   openai_estimated_cost=?, enrichment_error_summary=?,
                   circuit_breaker_state=? WHERE id=?""",
                (fetch_created, fetch_completed, fetch_retried, fetch_dead,
                 analysis_created, analysis_completed, analysis_retried,
                 analysis_blocked, analysis_dead, calls, input_tokens, output_tokens,
                 estimated_cost, "\n".join(errors) or None,
                 ",".join(state for state in circuit_breakers if state != "closed") or "closed",
                 run_id),
            )

    def upsert_job(
        self,
        row: dict,
        *,
        query_group: str,
        search_term: str,
    ) -> tuple[int, bool]:
        now = utc_now()
        source = str(clean_value(row.get("site")) or "unknown").casefold()
        source_job_id = str(clean_value(row.get("id")) or "").strip()
        job_url = str(clean_value(row.get("job_url")) or "").strip()
        direct_url = str(clean_value(row.get("job_url_direct")) or "").strip()
        normalized_url = normalize_url(direct_url or job_url)
        fingerprint = job_fingerprint(row)
        source_key = source_job_id or normalized_url or fingerprint
        title = str(clean_value(row.get("title")) or "").strip()
        company = str(clean_value(row.get("company") or row.get("company_name")) or "").strip()
        location = str(clean_value(row.get("location")) or "").strip()
        matched_terms = row.get("matched_terms") or []

        with self.connect() as connection:
            posting = connection.execute(
                "SELECT job_id FROM postings WHERE source = ? AND source_key = ?",
                (source, source_key),
            ).fetchone()
            job_id = int(posting["job_id"]) if posting else None

            if job_id is None:
                existing_job = connection.execute(
                    "SELECT id FROM jobs WHERE fingerprint = ?",
                    (fingerprint,),
                ).fetchone()
                job_id = int(existing_job["id"]) if existing_job else None

            is_new = job_id is None
            if is_new:
                cursor = connection.execute(
                    """
                    INSERT INTO jobs(
                        fingerprint, title, normalized_title, company, normalized_company,
                        location, normalized_location, is_remote, role_family, seniority,
                        eligible, eligibility_reason, date_posted, first_seen_at,
                        last_seen_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'accepted', ?, ?, ?, ?)
                    """,
                    (
                        fingerprint,
                        title,
                        normalize_text(title),
                        company or None,
                        normalize_text(company),
                        location or None,
                        normalize_text(location),
                        clean_value(row.get("is_remote")),
                        row.get("role_family"),
                        row.get("seniority", "unspecified"),
                        clean_value(row.get("date_posted")),
                        now,
                        now,
                        now,
                    ),
                )
                job_id = int(cursor.lastrowid)
            else:
                connection.execute(
                    """
                    UPDATE jobs SET
                        title = COALESCE(NULLIF(?, ''), title),
                        company = COALESCE(NULLIF(?, ''), company),
                        location = COALESCE(NULLIF(?, ''), location),
                        is_remote = COALESCE(?, is_remote),
                        date_posted = COALESCE(?, date_posted),
                        last_seen_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        title,
                        company,
                        location,
                        clean_value(row.get("is_remote")),
                        clean_value(row.get("date_posted")),
                        now,
                        now,
                        job_id,
                    ),
                )

            connection.execute(
                """
                INSERT INTO postings(
                    job_id, source, source_key, source_job_id, job_url, direct_url,
                    normalized_url, description, job_type, salary_interval,
                    min_amount, max_amount, currency, query_group, search_term,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_key) DO UPDATE SET
                    job_url = COALESCE(NULLIF(excluded.job_url, ''), postings.job_url),
                    direct_url = COALESCE(NULLIF(excluded.direct_url, ''), postings.direct_url),
                    description = COALESCE(NULLIF(excluded.description, ''), postings.description),
                    job_type = COALESCE(excluded.job_type, postings.job_type),
                    salary_interval = COALESCE(excluded.salary_interval, postings.salary_interval),
                    min_amount = COALESCE(excluded.min_amount, postings.min_amount),
                    max_amount = COALESCE(excluded.max_amount, postings.max_amount),
                    currency = COALESCE(excluded.currency, postings.currency),
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    job_id,
                    source,
                    source_key,
                    source_job_id or None,
                    job_url or None,
                    direct_url or None,
                    normalized_url or None,
                    clean_value(row.get("description")),
                    clean_value(row.get("job_type")),
                    clean_value(row.get("interval")),
                    clean_value(row.get("min_amount")),
                    clean_value(row.get("max_amount")),
                    clean_value(row.get("currency")),
                    query_group,
                    search_term,
                    now,
                    now,
                ),
            )

            for term in matched_terms:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO job_matches(job_id, role_family, matched_term, query_group)
                    VALUES (?, ?, ?, ?)
                    """,
                    (job_id, row.get("role_family"), term, query_group),
                )

        return job_id, is_new

    def set_status(self, job_id: int, new_status: str, note: str | None = None) -> None:
        if new_status not in VALID_STATUSES:
            raise ValueError(f"Status must be one of: {', '.join(VALID_STATUSES)}")
        now = utc_now()
        with self.connect() as connection:
            job = connection.execute(
                "SELECT status FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if job is None:
                raise KeyError(f"Job {job_id} was not found")
            applied_at = now if new_status == "applied" else None
            connection.execute(
                """
                UPDATE jobs SET status = ?, notes = COALESCE(?, notes),
                    applied_at = COALESCE(?, applied_at), updated_at = ?
                WHERE id = ?
                """,
                (new_status, note, applied_at, now, job_id),
            )
            connection.execute(
                """
                INSERT INTO status_history(job_id, old_status, new_status, note, changed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, job["status"], new_status, note, now),
            )
            if new_status == "rejected":
                connection.execute(
                    """UPDATE enrichment_tasks SET status='cancelled', updated_at=?
                       WHERE posting_id IN (SELECT id FROM postings WHERE job_id=?)
                         AND status IN ('pending','retry','leased','budget_blocked')""",
                    (now, job_id),
                )

    def reclassify_jobs(self) -> tuple[int, int]:
        """Reapply current title rules without deleting jobs or workflow history."""
        eligible_count = 0
        ineligible_count = 0
        now = utc_now()
        with self.connect() as connection:
            jobs = connection.execute("SELECT id, title FROM jobs").fetchall()
            for job in jobs:
                decision = classify_title(job["title"])
                if decision.accepted:
                    eligible_count += 1
                    connection.execute(
                        """
                        UPDATE jobs SET eligible = 1, eligibility_reason = 'accepted',
                            role_family = ?, seniority = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (decision.role_family, decision.seniority, now, job["id"]),
                    )
                else:
                    ineligible_count += 1
                    connection.execute(
                        """
                        UPDATE jobs SET eligible = 0, eligibility_reason = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (decision.reason, now, job["id"]),
                    )
                    connection.execute(
                        """UPDATE enrichment_tasks SET status='cancelled', updated_at=?
                           WHERE posting_id IN (SELECT id FROM postings WHERE job_id=?)
                             AND status IN ('pending','retry','leased','budget_blocked')""",
                        (now, job["id"]),
                    )
        return eligible_count, ineligible_count

    def list_jobs(self, status: str | None = None, limit: int = 50) -> list[sqlite3.Row]:
        query = "SELECT id, status, title, company, location, date_posted FROM jobs"
        parameters: list[object] = []
        if status:
            if status not in VALID_STATUSES:
                raise ValueError(f"Status must be one of: {', '.join(VALID_STATUSES)}")
            query += " WHERE status = ?"
            parameters.append(status)
        query += " ORDER BY COALESCE(date_posted, first_seen_at) DESC, id DESC LIMIT ?"
        parameters.append(limit)
        with self.connect() as connection:
            return list(connection.execute(query, parameters).fetchall())

    def export_rows(
        self,
        job_ids: list[int] | None = None,
        *,
        fresh_since: datetime | None = None,
    ) -> list[dict]:
        clauses = ["j.eligible = 1", "j.status != 'rejected'"]
        parameters: list[object] = []
        if job_ids is not None:
            if not job_ids:
                return []
            placeholders = ",".join("?" for _ in job_ids)
            clauses.append(f"j.id IN ({placeholders})")
            parameters.extend(job_ids)
        where = "WHERE " + " AND ".join(clauses)

        query = f"""
            SELECT
                j.id, j.status, j.title, j.company, j.location, j.is_remote,
                j.role_family, j.seniority, j.date_posted, j.first_seen_at,
                j.last_seen_at, j.notes,
                (SELECT group_concat(source, ', ')
                 FROM (SELECT DISTINCT source FROM postings WHERE job_id = j.id ORDER BY source)) AS sources,
                (SELECT COALESCE(direct_url, job_url) FROM postings
                 WHERE job_id = j.id ORDER BY CASE source WHEN 'linkedin' THEN 0 ELSE 1 END, id LIMIT 1) AS preferred_url,
                (SELECT min_amount FROM postings WHERE job_id = j.id AND min_amount IS NOT NULL LIMIT 1) AS min_amount,
                (SELECT max_amount FROM postings WHERE job_id = j.id AND max_amount IS NOT NULL LIMIT 1) AS max_amount,
                (SELECT currency FROM postings WHERE job_id = j.id AND currency IS NOT NULL LIMIT 1) AS currency,
                (SELECT salary_interval FROM postings WHERE job_id = j.id AND salary_interval IS NOT NULL LIMIT 1) AS salary_interval,
                (SELECT group_concat(matched_term, ', ')
                 FROM (SELECT DISTINCT matched_term FROM job_matches WHERE job_id = j.id ORDER BY matched_term)) AS matched_terms
            FROM jobs j
            {where}
            ORDER BY
                CASE j.status WHEN 'new' THEN 0 WHEN 'saved' THEN 1 WHEN 'reviewed' THEN 2
                    WHEN 'applied' THEN 3 ELSE 4 END,
                COALESCE(j.date_posted, j.first_seen_at) DESC,
                j.id DESC
        """
        with self.connect() as connection:
            rows = [dict(row) for row in connection.execute(query, parameters).fetchall()]
            if fresh_since is not None:
                cutoff = _as_utc(fresh_since)
                rows = [row for row in rows if _is_current_job(row, cutoff)]
            selected_ids = [row["id"] for row in rows]
            requirements_by_job = self._current_requirements(connection, selected_ids)
            postings_by_job = self._posting_states(connection, selected_ids)
            for row in rows:
                requirements = requirements_by_job.get(row["id"], [])
                grouped: dict[str, list[str]] = {}
                for requirement in requirements:
                    key = requirement["requirement_type"]
                    if key == "skill":
                        key = f"{requirement['priority']}_skills"
                    grouped.setdefault(key, [])
                    if requirement["canonical_value"] not in grouped[key]:
                        grouped[key].append(requirement["canonical_value"])
                for key in (
                    "required_skills", "preferred_skills", "experience",
                ):
                    row[key] = "; ".join(grouped.get(key, []))
                states = [
                    posting["analysis_status"]
                    for posting in postings_by_job.get(row["id"], [])
                ]
                row["analysis_status"] = min(
                    states or ["unavailable"],
                    key=ANALYSIS_STATUS_PRIORITY.__getitem__,
                )
            return rows

    @staticmethod
    def _id_chunks(values: list[int], size: int = 500) -> Iterator[list[int]]:
        for start in range(0, len(values), size):
            yield values[start:start + size]

    def _current_requirements(
        self, connection: sqlite3.Connection, job_ids: list[int],
    ) -> dict[int, list[dict]]:
        grouped: dict[int, list[dict]] = {}
        for chunk in self._id_chunks(job_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""SELECT p.job_id,p.id posting_id,p.source,r.requirement_type,
                           r.canonical_value,r.category,r.priority,r.evidence
                    FROM posting_requirements r
                    JOIN posting_analyses a ON a.id=r.analysis_id
                    JOIN postings p ON p.id=a.posting_id
                    WHERE p.job_id IN ({placeholders})
                      AND a.description_hash=p.description_hash
                      AND a.model=? AND a.prompt_version=?
                    ORDER BY p.job_id,p.id,r.requirement_type,r.priority,
                             r.canonical_value""",
                [*chunk, config.LLM_MODEL, config.LLM_PROMPT_VERSION],
            ).fetchall()
            for source_row in rows:
                row = dict(source_row)
                grouped.setdefault(row["job_id"], []).append(row)
        return grouped

    def _posting_states(
        self, connection: sqlite3.Connection, job_ids: list[int],
    ) -> dict[int, list[dict]]:
        postings: list[dict] = []
        tasks_by_posting: dict[int, list[dict]] = {}
        for chunk in self._id_chunks(job_ids):
            placeholders = ",".join("?" for _ in chunk)
            posting_rows = connection.execute(
                f"""SELECT p.id posting_id,p.job_id,p.source,
                           COALESCE(NULLIF(p.job_url,''),NULLIF(p.normalized_url,''),
                                    NULLIF(p.direct_url,'')) source_url,
                           COALESCE(NULLIF(p.direct_url,''),NULLIF(p.job_url,'')) url,
                           p.description,
                           p.description_hash,p.description_source,
                           p.description_fetched_at,
                           EXISTS(
                               SELECT 1 FROM posting_analyses a
                               WHERE a.posting_id=p.id
                                 AND a.description_hash=p.description_hash
                                 AND a.model=? AND a.prompt_version=?
                           ) analysis_completed
                    FROM postings p WHERE p.job_id IN ({placeholders})
                    ORDER BY p.job_id,p.source,p.id""",
                [config.LLM_MODEL, config.LLM_PROMPT_VERSION, *chunk],
            ).fetchall()
            postings.extend(dict(row) for row in posting_rows)
            posting_ids = [row["posting_id"] for row in posting_rows]
            for posting_chunk in self._id_chunks(posting_ids):
                task_placeholders = ",".join("?" for _ in posting_chunk)
                task_rows = connection.execute(
                    f"""SELECT id,identity_key,posting_id,task_type,status,
                               description_hash,model,prompt_version,attempt_count,
                               last_error_class,last_error_message,next_attempt_at
                        FROM enrichment_tasks
                        WHERE posting_id IN ({task_placeholders})
                        ORDER BY id DESC""",
                    posting_chunk,
                ).fetchall()
                for source_row in task_rows:
                    task = dict(source_row)
                    tasks_by_posting.setdefault(task["posting_id"], []).append(task)

        grouped: dict[int, list[dict]] = {}
        for posting in postings:
            posting_tasks = tasks_by_posting.get(posting["posting_id"], [])
            description = str(posting["description"] or "").strip()
            task: dict | None = None
            state: str
            if posting["analysis_completed"]:
                state = "completed"
                task = next((item for item in posting_tasks if (
                    item["task_type"] == "analyze_description"
                    and item["description_hash"] == posting["description_hash"]
                    and item["model"] == config.LLM_MODEL
                    and item["prompt_version"] == config.LLM_PROMPT_VERSION
                )), None)
            elif description:
                task = next((item for item in posting_tasks if (
                    item["task_type"] == "analyze_description"
                    and item["description_hash"] == posting["description_hash"]
                    and item["model"] == config.LLM_MODEL
                    and item["prompt_version"] == config.LLM_PROMPT_VERSION
                )), None)
                state = self._task_analysis_state(task, completed="not_queued")
            elif posting["source"] == "linkedin" and posting["source_url"]:
                identity_url = normalize_url(posting["source_url"])
                identity = f"fetch:{posting['posting_id']}:" + hashlib.sha256(
                    identity_url.encode()
                ).hexdigest()
                task = next((item for item in posting_tasks if (
                    item["task_type"] == "fetch_description"
                    and item["identity_key"] == identity
                )), None)
                state = self._task_analysis_state(task, completed="unavailable")
            else:
                state = "unavailable"
            detail = dict(posting)
            detail.pop("description", None)
            detail["description_available"] = bool(description)
            detail["analysis_status"] = state
            detail["task_id"] = task["id"] if task else None
            detail["task_type"] = task["task_type"] if task else None
            detail["task_status"] = task["status"] if task else None
            detail["attempt_count"] = task["attempt_count"] if task else 0
            detail["last_error_class"] = task["last_error_class"] if task else None
            detail["last_error_message"] = task["last_error_message"] if task else None
            detail["next_attempt_at"] = task["next_attempt_at"] if task else None
            grouped.setdefault(posting["job_id"], []).append(detail)
        return grouped

    @staticmethod
    def _task_analysis_state(
        task: dict | None, *, completed: str = "completed",
    ) -> str:
        if task is None or task["status"] == "cancelled":
            return "not_queued"
        if task["status"] == "leased":
            return "pending"
        if task["status"] == "completed":
            return completed
        if task["status"] in ANALYSIS_STATUS_PRIORITY:
            return task["status"]
        return "not_queued"

    def get_job_details(self, job_id: int) -> dict:
        with self.connect() as connection:
            job = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(f"Job {job_id} was not found")
            postings = self._posting_states(connection, [job_id]).get(job_id, [])
            requirements = self._current_requirements(connection, [job_id]).get(job_id, [])
        return {"job": dict(job), "postings": postings, "requirements": requirements}

    def get_run(self, run_id: int) -> dict:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM scrape_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Run {run_id} was not found")
            return dict(row)
