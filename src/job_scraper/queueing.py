"""Durable enrichment queue and usage accounting backed by SQLite."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import TYPE_CHECKING, Iterable
from uuid import uuid4

from . import config

if TYPE_CHECKING:
    from .storage import JobStore


TASK_STATUSES = (
    "pending", "leased", "retry", "completed", "dead", "cancelled",
    "budget_blocked",
)
TASK_TYPES = ("fetch_description", "analyze_description")


def _now(value: datetime | None = None) -> datetime:
    return (value or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        microsecond=0
    )


def _iso(value: datetime | None = None) -> str:
    return _now(value).isoformat()


def prepare_description(value: str) -> str:
    # JobSpy descriptions can contain Markdown escape characters (for example
    # ``2\+ years``). Remove only escapes for punctuation and retain section
    # boundaries so the analyzer can distinguish required/preferred headings.
    value = re.sub(r"\\([\\`*_{}\[\]()#+\-.!])", r"\1", value or "")
    lines = [" ".join(line.split()) for line in value.splitlines()]
    prepared: list[str] = []
    for line in lines:
        if line or (prepared and prepared[-1]):
            prepared.append(line)
    return "\n".join(prepared).strip()


def normalize_description(value: str) -> str:
    return " ".join(prepare_description(value).split())


def description_hash(value: str) -> str:
    return hashlib.sha256(normalize_description(value).encode()).hexdigest()


def initialize_enrichment_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS enrichment_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identity_key TEXT NOT NULL UNIQUE,
            task_type TEXT NOT NULL CHECK(task_type IN
                ('fetch_description', 'analyze_description')),
            posting_id INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
            scrape_run_id INTEGER REFERENCES scrape_runs(id) ON DELETE SET NULL,
            description_hash TEXT,
            model TEXT,
            prompt_version TEXT,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN
                ('pending','leased','retry','completed','dead','cancelled','budget_blocked')),
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT NOT NULL,
            lease_token TEXT,
            lease_expires_at TEXT,
            last_error_class TEXT,
            last_error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS posting_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            posting_id INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
            description_hash TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            UNIQUE(posting_id, description_hash, model, prompt_version)
        );

        CREATE TABLE IF NOT EXISTS enrichment_task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL REFERENCES enrichment_tasks(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            from_status TEXT,
            to_status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            error_class TEXT,
            error_message TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS posting_requirements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id INTEGER NOT NULL REFERENCES posting_analyses(id) ON DELETE CASCADE,
            posting_id INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
            requirement_type TEXT NOT NULL,
            canonical_value TEXT NOT NULL,
            category TEXT,
            priority TEXT NOT NULL CHECK(priority IN ('required','preferred','mentioned')),
            structured_value TEXT,
            evidence TEXT NOT NULL,
            UNIQUE(analysis_id, requirement_type, canonical_value)
        );

        CREATE TABLE IF NOT EXISTS llm_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reservation_token TEXT NOT NULL UNIQUE,
            task_id INTEGER NOT NULL REFERENCES enrichment_tasks(id) ON DELETE CASCADE,
            month_key TEXT NOT NULL,
            model TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('reserved','completed','failed','unknown')),
            input_tokens INTEGER,
            output_tokens INTEGER,
            projected_cost_usd REAL NOT NULL,
            actual_cost_usd REAL,
            pricing_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_enrichment_due
            ON enrichment_tasks(task_type, status, next_attempt_at, created_at, id);
        CREATE INDEX IF NOT EXISTS idx_enrichment_posting
            ON enrichment_tasks(posting_id, task_type, status);
        CREATE INDEX IF NOT EXISTS idx_enrichment_events
            ON enrichment_task_events(task_id, created_at, id);
        CREATE INDEX IF NOT EXISTS idx_analysis_current
            ON posting_analyses(posting_id, description_hash);
        CREATE INDEX IF NOT EXISTS idx_usage_month ON llm_usage(month_key, status);
        """
    )
    columns = {r["name"] for r in connection.execute("PRAGMA table_info(postings)")}
    for name, definition in (
        ("description_hash", "TEXT"),
        ("description_source", "TEXT"),
        ("description_fetched_at", "TEXT"),
    ):
        if name not in columns:
            connection.execute(f"ALTER TABLE postings ADD COLUMN {name} {definition}")


class EnrichmentQueue:
    def __init__(self, store: JobStore, *, migrate_existing: bool = False):
        self.store = store
        with store.connect() as connection:
            initialize_enrichment_schema(connection)
        if migrate_existing:
            self.migrate_existing()

    @staticmethod
    def _fetch_identity(posting_id: int, url: str) -> str:
        digest = hashlib.sha256(url.encode()).hexdigest()
        return f"fetch:{posting_id}:{digest}"

    @staticmethod
    def _analysis_identity(posting_id: int, digest: str, model: str, version: str) -> str:
        return f"analyze:{posting_id}:{digest}:{model}:{version}"

    def _create_task(
        self, connection: sqlite3.Connection, *, task_type: str, posting_id: int,
        identity: str, run_id: int | None, digest: str | None = None,
        model: str | None = None, version: str | None = None,
    ) -> tuple[int, bool]:
        now = _iso()
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO enrichment_tasks(
                identity_key, task_type, posting_id, scrape_run_id,
                description_hash, model, prompt_version, next_attempt_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (identity, task_type, posting_id, run_id, digest, model, version,
             now, now, now),
        )
        row = connection.execute(
            "SELECT id,status FROM enrichment_tasks WHERE identity_key = ?", (identity,)
        ).fetchone()
        if cursor.rowcount != 1 and row["status"] == "cancelled":
            connection.execute(
                """UPDATE enrichment_tasks SET status='pending', attempt_count=0,
                   next_attempt_at=?, lease_token=NULL, lease_expires_at=NULL,
                   last_error_class=NULL, last_error_message=NULL, completed_at=NULL,
                   updated_at=? WHERE id=?""",
                (now, now, row["id"]),
            )
        return int(row["id"]), cursor.rowcount == 1

    def sync_posting(self, posting_id: int, run_id: int | None = None) -> dict[str, int]:
        counts = {"fetch_created": 0, "analysis_created": 0, "cancelled": 0}
        with self.store.connect() as connection:
            posting = connection.execute(
                """SELECT p.*, j.eligible, j.status AS job_status
                   FROM postings p JOIN jobs j ON j.id=p.job_id WHERE p.id=?""",
                (posting_id,),
            ).fetchone()
            if posting is None:
                return counts
            if not posting["eligible"] or posting["job_status"] == "rejected":
                cursor = connection.execute(
                    """UPDATE enrichment_tasks SET status='cancelled', updated_at=?,
                       lease_token=NULL, lease_expires_at=NULL
                       WHERE posting_id=? AND status IN ('pending','retry','leased','budget_blocked')""",
                    (_iso(), posting_id),
                )
                counts["cancelled"] = cursor.rowcount
                return counts

            description = normalize_description(posting["description"] or "")
            if description:
                digest = description_hash(description)
                connection.execute(
                    """UPDATE postings SET description_hash=?,
                       description_source=COALESCE(description_source, 'scrape')
                       WHERE id=?""",
                    (digest, posting_id),
                )
                cursor = connection.execute(
                    """UPDATE enrichment_tasks SET status='cancelled', updated_at=?
                       WHERE posting_id=? AND task_type='analyze_description'
                         AND (description_hash != ? OR model != ? OR prompt_version != ?)
                         AND status IN ('pending','retry','budget_blocked')""",
                    (_iso(), posting_id, digest, config.LLM_MODEL,
                     config.LLM_PROMPT_VERSION),
                )
                counts["cancelled"] = cursor.rowcount
                _, made = self._create_task(
                    connection, task_type="analyze_description", posting_id=posting_id,
                    identity=self._analysis_identity(
                        posting_id, digest, config.LLM_MODEL, config.LLM_PROMPT_VERSION
                    ),
                    run_id=run_id, digest=digest, model=config.LLM_MODEL,
                    version=config.LLM_PROMPT_VERSION,
                )
                counts["analysis_created"] = int(made)
            elif posting["source"] == "linkedin":
                # Fetch from the source listing, not a company redirect stored as
                # job_url_direct; the public LinkedIn page owns the description.
                url = posting["job_url"] or posting["normalized_url"] or posting["direct_url"] or ""
                if url:
                    from .storage import normalize_url
                    identity_url = normalize_url(url)
                    _, made = self._create_task(
                        connection, task_type="fetch_description", posting_id=posting_id,
                        identity=self._fetch_identity(posting_id, identity_url), run_id=run_id,
                    )
                    counts["fetch_created"] = int(made)
        return counts

    def sync_job(self, job_id: int, run_id: int | None = None) -> dict[str, int]:
        total = {"fetch_created": 0, "analysis_created": 0, "cancelled": 0}
        with self.store.connect() as connection:
            ids = [r[0] for r in connection.execute(
                "SELECT id FROM postings WHERE job_id=?", (job_id,)
            )]
        for posting_id in ids:
            result = self.sync_posting(posting_id, run_id)
            for key in total:
                total[key] += result[key]
        return total

    def migrate_existing(self) -> dict[str, int]:
        total = {"fetch_created": 0, "analysis_created": 0, "cancelled": 0}
        with self.store.connect() as connection:
            ids = [r[0] for r in connection.execute(
                """SELECT p.id FROM postings p JOIN jobs j ON j.id=p.job_id
                   WHERE j.eligible=1 AND j.status!='rejected'"""
            )]
        for posting_id in ids:
            result = self.sync_posting(posting_id)
            for key in total:
                total[key] += result[key]
        return total

    def repair_orphans(
        self, *, fetch_limit: int, analysis_limit: int,
    ) -> dict[str, int]:
        """Queue a bounded set of postings that have no current work or result."""
        total = {"fetch_created": 0, "analysis_created": 0, "cancelled": 0}
        with self.store.connect() as connection:
            analysis_ids = [] if analysis_limit <= 0 else [
                row[0] for row in connection.execute(
                    """SELECT p.id
                       FROM postings p JOIN jobs j ON j.id=p.job_id
                       WHERE j.eligible=1 AND j.status!='rejected'
                         AND TRIM(COALESCE(p.description,''))!=''
                         AND NOT EXISTS (
                             SELECT 1 FROM posting_analyses a
                             WHERE a.posting_id=p.id
                               AND a.description_hash=p.description_hash
                               AND a.model=? AND a.prompt_version=?
                         )
                         AND NOT EXISTS (
                             SELECT 1 FROM enrichment_tasks t
                             WHERE t.posting_id=p.id
                               AND t.task_type='analyze_description'
                               AND t.description_hash=p.description_hash
                               AND t.model=? AND t.prompt_version=?
                         )
                       ORDER BY p.last_seen_at DESC,p.id
                       LIMIT ?""",
                    (
                        config.LLM_MODEL, config.LLM_PROMPT_VERSION,
                        config.LLM_MODEL, config.LLM_PROMPT_VERSION,
                        analysis_limit,
                    ),
                )
            ]
            fetch_ids = [] if fetch_limit <= 0 else [
                row[0] for row in connection.execute(
                    """SELECT p.id
                       FROM postings p JOIN jobs j ON j.id=p.job_id
                       WHERE j.eligible=1 AND j.status!='rejected'
                         AND p.source='linkedin'
                         AND TRIM(COALESCE(p.description,''))=''
                         AND TRIM(COALESCE(NULLIF(p.job_url,''),
                             NULLIF(p.normalized_url,''),NULLIF(p.direct_url,''),''))!=''
                         AND NOT EXISTS (
                             SELECT 1 FROM enrichment_tasks t
                             WHERE t.posting_id=p.id AND t.task_type='fetch_description'
                         )
                       ORDER BY p.last_seen_at DESC,p.id
                       LIMIT ?""",
                    (fetch_limit,),
                )
            ]
        for posting_id in [*analysis_ids, *fetch_ids]:
            result = self.sync_posting(posting_id)
            for key in total:
                total[key] += result[key]
        return total

    def _reclaim_expired(self, connection: sqlite3.Connection, now: datetime) -> None:
        rows = connection.execute(
            """SELECT id, attempt_count FROM enrichment_tasks
               WHERE status='leased' AND lease_expires_at <= ?""", (_iso(now),)
        ).fetchall()
        for row in rows:
            dead = row["attempt_count"] >= config.ENRICHMENT_MAX_ATTEMPTS
            delay = 0 if dead else (3600 if row["attempt_count"] == 1 else 21600)
            connection.execute(
                """UPDATE enrichment_tasks SET status=?, next_attempt_at=?,
                   lease_token=NULL, lease_expires_at=NULL,
                   last_error_class='LeaseExpired',
                   last_error_message='Worker lease expired before completion', updated_at=?
                   WHERE id=?""",
                ("dead" if dead else "retry", _iso(now + timedelta(seconds=delay)),
                 _iso(now), row["id"]),
            )
            connection.execute(
                """INSERT INTO enrichment_task_events(task_id,event_type,from_status,to_status,
                   attempt_count,error_class,error_message,created_at)
                   VALUES(?, 'lease_expired', 'leased', ?, ?, 'LeaseExpired',
                   'Worker lease expired before completion', ?)""",
                (row["id"], "dead" if dead else "retry", row["attempt_count"], _iso(now)),
            )

    def claim(
        self, task_type: str, limit: int, *, current_run_id: int | None = None,
        now: datetime | None = None, current_run_share: int | None = None,
    ) -> list[dict]:
        if task_type not in TASK_TYPES or limit <= 0:
            return []
        moment = _now(now)
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._reclaim_expired(connection, moment)
            month = moment.strftime("%Y-%m")
            connection.execute(
                """UPDATE enrichment_tasks SET status='pending', updated_at=?
                   WHERE status='budget_blocked'
                     AND substr(updated_at,1,7) != ?""",
                (_iso(moment), month),
            )
            due = "status IN ('pending','retry') AND next_attempt_at <= ?"
            due_at = _iso(moment)

            def select_rows(condition: str, values: list[object], amount: int) -> list:
                if amount <= 0:
                    return []
                return list(connection.execute(
                    f"""SELECT * FROM enrichment_tasks
                         WHERE task_type=? AND {due} {condition}
                         ORDER BY next_attempt_at, created_at, id LIMIT ?""",
                    [task_type, due_at, *values, amount],
                ).fetchall())

            if current_run_id is None:
                selected = select_rows("", [], limit)
            else:
                share = config.ENRICHMENT_CURRENT_RUN_SHARE if current_run_share is None else current_run_share
                share = max(0, min(100, share))
                current_quota = min(limit, round(limit * share / 100))
                backlog_quota = max(0, limit - current_quota)
                selected = select_rows(
                    "AND scrape_run_id=?", [current_run_id], current_quota
                )
                selected += select_rows(
                    "AND (scrape_run_id IS NULL OR scrape_run_id!=?)",
                    [current_run_id], backlog_quota,
                )
                remaining = limit - len(selected)
                if remaining:
                    selected_ids = [row["id"] for row in selected]
                    exclusion = ""
                    values: list[object] = []
                    if selected_ids:
                        placeholders = ",".join("?" for _ in selected_ids)
                        exclusion = f"AND id NOT IN ({placeholders})"
                        values.extend(selected_ids)
                    selected += select_rows(exclusion, values, remaining)
            claimed = []
            for row in selected:
                token = uuid4().hex
                expires = moment + timedelta(seconds=config.ENRICHMENT_LEASE_SECONDS)
                connection.execute(
                    """UPDATE enrichment_tasks SET status='leased', attempt_count=attempt_count+1,
                       lease_token=?, lease_expires_at=?, updated_at=? WHERE id=?""",
                    (token, _iso(expires), _iso(moment), row["id"]),
                )
                item = dict(row)
                item.update(lease_token=token, lease_expires_at=_iso(expires),
                            attempt_count=row["attempt_count"] + 1)
                claimed.append(item)
            return claimed

    def task_context(self, task_id: int) -> dict:
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT t.*, p.job_id, p.source, p.description, p.description_hash AS current_hash,
                          CASE WHEN p.source='linkedin' THEN COALESCE(p.job_url,p.direct_url)
                               ELSE COALESCE(p.direct_url,p.job_url) END AS url,
                          j.title, j.company
                   FROM enrichment_tasks t JOIN postings p ON p.id=t.posting_id
                   JOIN jobs j ON j.id=p.job_id WHERE t.id=?""", (task_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"Task {task_id} was not found")
        return dict(row)

    def complete_fetch(self, task_id: int, token: str, description: str) -> tuple[bool, bool]:
        text = normalize_description(description)
        if not text:
            raise ValueError("Description was empty")
        digest = description_hash(text)
        now = _iso()
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            task = connection.execute(
                "SELECT * FROM enrichment_tasks WHERE id=? AND status='leased' AND lease_token=?",
                (task_id, token),
            ).fetchone()
            if task is None:
                return False, False
            connection.execute(
                """UPDATE postings SET description=?, description_hash=?,
                   description_source='linkedin_fetch', description_fetched_at=? WHERE id=?""",
                (text, digest, now, task["posting_id"]),
            )
            connection.execute(
                """UPDATE enrichment_tasks SET status='cancelled', updated_at=?
                   WHERE posting_id=? AND task_type='analyze_description'
                     AND description_hash != ? AND status IN ('pending','retry','budget_blocked')""",
                (now, task["posting_id"], digest),
            )
            _, analysis_created = self._create_task(
                connection, task_type="analyze_description", posting_id=task["posting_id"],
                identity=self._analysis_identity(task["posting_id"], digest,
                    config.LLM_MODEL, config.LLM_PROMPT_VERSION),
                run_id=task["scrape_run_id"], digest=digest, model=config.LLM_MODEL,
                version=config.LLM_PROMPT_VERSION,
            )
            connection.execute(
                """UPDATE enrichment_tasks SET status='completed', completed_at=?, updated_at=?,
                   lease_token=NULL, lease_expires_at=NULL WHERE id=?""", (now, now, task_id)
            )
        return True, analysis_created

    def complete_analysis(self, task_id: int, token: str, requirements: Iterable[dict]) -> bool:
        now = _iso()
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            task = connection.execute(
                "SELECT * FROM enrichment_tasks WHERE id=? AND status='leased' AND lease_token=?",
                (task_id, token),
            ).fetchone()
            if task is None:
                return False
            cursor = connection.execute(
                """INSERT OR IGNORE INTO posting_analyses(posting_id,description_hash,model,
                   prompt_version,created_at,completed_at) VALUES(?,?,?,?,?,?)""",
                (task["posting_id"], task["description_hash"], task["model"],
                 task["prompt_version"], now, now),
            )
            analysis = connection.execute(
                """SELECT id FROM posting_analyses WHERE posting_id=? AND description_hash=?
                   AND model=? AND prompt_version=?""",
                (task["posting_id"], task["description_hash"], task["model"],
                 task["prompt_version"]),
            ).fetchone()
            analysis_id = int(analysis["id"])
            for item in requirements:
                connection.execute(
                    """INSERT INTO posting_requirements(analysis_id,posting_id,requirement_type,
                       canonical_value,category,priority,structured_value,evidence)
                       VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(analysis_id,requirement_type,canonical_value)
                       DO UPDATE SET priority=excluded.priority, category=excluded.category,
                       structured_value=excluded.structured_value, evidence=excluded.evidence""",
                    (analysis_id, task["posting_id"], item["type"], item["value"],
                     item.get("category"), item["priority"],
                     json.dumps(item.get("structured"), sort_keys=True) if item.get("structured") else None,
                     item["evidence"]),
                )
            connection.execute(
                """UPDATE enrichment_tasks SET status='completed', completed_at=?, updated_at=?,
                   lease_token=NULL, lease_expires_at=NULL WHERE id=?""", (now, now, task_id)
            )
        return cursor.rowcount >= 0

    def fail(self, task_id: int, token: str, error: Exception, *, permanent: bool = False,
             retry_after: int | None = None, preserve_attempt: bool = False) -> str:
        moment = _now()
        with self.store.connect() as connection:
            task = connection.execute(
                "SELECT attempt_count FROM enrichment_tasks WHERE id=? AND status='leased' AND lease_token=?",
                (task_id, token),
            ).fetchone()
            if task is None:
                return "lost_lease"
            attempts = task["attempt_count"] - int(preserve_attempt)
            if preserve_attempt:
                status, delay = "pending", 0
            elif permanent or attempts >= config.ENRICHMENT_MAX_ATTEMPTS:
                status, delay = "dead", 0
            else:
                status = "retry"
                delay = retry_after if retry_after is not None else (3600 if attempts == 1 else 21600)
            connection.execute(
                """UPDATE enrichment_tasks SET status=?, attempt_count=?, next_attempt_at=?,
                   lease_token=NULL, lease_expires_at=NULL, last_error_class=?,
                   last_error_message=?, updated_at=? WHERE id=?""",
                (status, attempts, _iso(moment + timedelta(seconds=delay)),
                 type(error).__name__, str(error)[:1000], _iso(moment), task_id),
            )
            connection.execute(
                """INSERT INTO enrichment_task_events(task_id,event_type,from_status,to_status,
                   attempt_count,error_class,error_message,created_at)
                   VALUES(?, 'failure', 'leased', ?, ?, ?, ?, ?)""",
                (task_id, status, attempts, type(error).__name__, str(error)[:1000], _iso(moment)),
            )
            return status

    def mark_budget_blocked(self) -> int:
        with self.store.connect() as connection:
            cursor = connection.execute(
                """UPDATE enrichment_tasks SET status='budget_blocked', updated_at=?
                   WHERE task_type='analyze_description' AND status IN ('pending','retry')
                     AND next_attempt_at <= ?""", (_iso(), _iso())
            )
            return cursor.rowcount

    def reserve_usage(self, task_id: int, model: str, projected: float) -> str:
        token, now = uuid4().hex, _now()
        with self.store.connect() as connection:
            connection.execute(
                """INSERT INTO llm_usage(reservation_token,task_id,month_key,model,status,
                   projected_cost_usd,pricing_version,created_at,updated_at)
                   VALUES(?,?,?,?, 'reserved', ?,?,?,?)""",
                (token, task_id, now.strftime("%Y-%m"), model, projected,
                 config.LLM_PRICING_VERSION, _iso(now), _iso(now)),
            )
        return token

    def finish_usage(self, token: str, *, input_tokens: int = 0, output_tokens: int = 0,
                     status: str = "completed") -> None:
        actual = (input_tokens * config.LLM_INPUT_PRICE_PER_MILLION +
                  output_tokens * config.LLM_OUTPUT_PRICE_PER_MILLION) / 1_000_000
        with self.store.connect() as connection:
            connection.execute(
                """UPDATE llm_usage SET status=?, input_tokens=?, output_tokens=?,
                   actual_cost_usd=?, updated_at=? WHERE reservation_token=?""",
                (status, input_tokens, output_tokens,
                 actual if status == "completed" else None, _iso(), token),
            )

    def usage(self, month: str | None = None) -> dict:
        month = month or _now().strftime("%Y-%m")
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT COUNT(*) calls, COALESCE(SUM(input_tokens),0) input_tokens,
                   COALESCE(SUM(output_tokens),0) output_tokens,
                   COALESCE(SUM(CASE WHEN status='completed' THEN actual_cost_usd
                       WHEN status IN ('reserved','unknown') THEN projected_cost_usd ELSE 0 END),0) cost
                   FROM llm_usage WHERE month_key=?""", (month,)
            ).fetchone()
        return dict(row) | {"month": month}

    def outstanding_usage(self) -> list[dict]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT reservation_token,task_id,month_key,model,status,projected_cost_usd,
                   created_at FROM llm_usage WHERE status IN ('reserved','unknown')
                   ORDER BY created_at,id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def resolve_usage(self, token: str, actual_cost: float) -> None:
        if actual_cost < 0:
            raise ValueError("Actual cost cannot be negative")
        with self.store.connect() as connection:
            cursor = connection.execute(
                """UPDATE llm_usage SET status='completed', actual_cost_usd=?, updated_at=?
                   WHERE reservation_token=? AND status IN ('reserved','unknown')""",
                (actual_cost, _iso(), token),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Outstanding reservation {token} was not found")

    def queue_summary(self) -> list[dict]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT task_type,status,COUNT(*) count FROM enrichment_tasks
                   GROUP BY task_type,status ORDER BY task_type,status"""
            ).fetchall()
        return [dict(r) for r in rows]

    def list_tasks(self, status: str, limit: int = 50) -> list[dict]:
        if status not in TASK_STATUSES:
            raise ValueError(f"Status must be one of: {', '.join(TASK_STATUSES)}")
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT t.id,t.task_type,t.status,t.attempt_count,t.posting_id,p.job_id,
                   t.last_error_class,t.last_error_message,t.next_attempt_at
                   FROM enrichment_tasks t JOIN postings p ON p.id=t.posting_id
                   WHERE t.status=? ORDER BY t.updated_at,t.id LIMIT ?""", (status, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    def retry(self, *, task_id: int | None = None, job_id: int | None = None) -> int:
        if (task_id is None) == (job_id is None):
            raise ValueError("Specify exactly one task ID or job ID")
        clause, value = ("id", task_id) if task_id is not None else (
            "posting_id IN (SELECT id FROM postings WHERE job_id=?)", job_id
        )
        sql = f"""UPDATE enrichment_tasks SET status='pending', attempt_count=0,
                  next_attempt_at=?, lease_token=NULL, lease_expires_at=NULL, updated_at=?
                  WHERE status='dead' AND {clause}{'=?' if task_id is not None else ''}"""
        with self.store.connect() as connection:
            matching = [row[0] for row in connection.execute(
                f"SELECT id FROM enrichment_tasks WHERE status='dead' AND {clause}{'=?' if task_id is not None else ''}",
                (value,),
            )]
            cursor = connection.execute(sql, (_iso(), _iso(), value))
            for matched_id in matching:
                connection.execute(
                    """INSERT INTO enrichment_task_events(task_id,event_type,from_status,to_status,
                       attempt_count,created_at) VALUES(?, 'manual_retry', 'dead', 'pending', 0, ?)""",
                    (matched_id, _iso()),
                )
            return cursor.rowcount
