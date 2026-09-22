"""Deterministic CSV exports derived from the SQLite store."""

import csv
from datetime import datetime
import os
from pathlib import Path
import tempfile

from .storage import JobStore


EXPORT_COLUMNS = [
    "id",
    "status",
    "title",
    "preferred_url",
    "company",
    "location",
    "is_remote",
    "role_family",
    "seniority",
    "date_posted",
    "first_seen_at",
    "last_seen_at",
    "sources",
    "compensation",
    "matched_terms",
    "required_skills",
    "preferred_skills",
    "experience",
    "analysis_status",
    "notes",
]


def _compensation(row: dict) -> str:
    minimum = row.pop("min_amount", None)
    maximum = row.pop("max_amount", None)
    currency = row.pop("currency", None) or ""
    interval = row.pop("salary_interval", None) or ""
    if minimum is None and maximum is None:
        return ""
    if minimum is not None and maximum is not None:
        amount = f"{minimum:g}-{maximum:g}"
    else:
        amount = f"{(minimum if minimum is not None else maximum):g}"
    suffix = f"/{interval}" if interval else ""
    return f"{currency} {amount}{suffix}".strip()


def _prepare_rows(rows: list[dict]) -> list[dict]:
    prepared = []
    for source_row in rows:
        row = dict(source_row)
        remote = row.get("is_remote")
        row["is_remote"] = "" if remote is None else ("yes" if remote else "no")
        row["compensation"] = _compensation(row)
        prepared.append({column: row.get(column, "") or "" for column in EXPORT_COLUMNS})
    return prepared


def write_csv(path: str | Path, rows: list[dict]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", newline="", encoding="utf-8", dir=target.parent,
            prefix=f".{target.name}.", suffix=".tmp", delete=False,
        ) as csv_file:
            temporary_path = Path(csv_file.name)
            writer = csv.DictWriter(csv_file, fieldnames=EXPORT_COLUMNS)
            writer.writeheader()
            writer.writerows(_prepare_rows(rows))
            csv_file.flush()
            os.fsync(csv_file.fileno())
        os.replace(temporary_path, target)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
    return target


def export_current_jobs(store: JobStore, export_dir: str | Path) -> Path:
    return write_csv(Path(export_dir) / "current-jobs.csv", store.export_rows())


def export_new_jobs(
    store: JobStore,
    export_dir: str | Path,
    job_ids: list[int],
    timestamp: datetime | None = None,
) -> Path | None:
    if not job_ids:
        return None
    timestamp = timestamp or datetime.now()
    path = Path(export_dir) / "runs" / f"new-jobs-{timestamp:%Y-%m-%d-%H-%M-%S}.csv"
    return write_csv(path, store.export_rows(job_ids))


def export_all(
    store: JobStore,
    export_dir: str | Path,
    new_job_ids: list[int] | None = None,
    timestamp: datetime | None = None,
) -> tuple[Path, Path | None]:
    store.reclassify_jobs()
    current_path = export_current_jobs(store, export_dir)
    run_path = export_new_jobs(store, export_dir, new_job_ids or [], timestamp)
    return current_path, run_path
