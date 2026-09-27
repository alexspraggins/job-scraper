"""Process-boundary tests for the installed CLI workflow."""

import csv
import os
from pathlib import Path
import subprocess
import sys

from job_scraper.storage import JobStore


REPO_ROOT = Path(__file__).resolve().parents[1]


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    source_path = str(REPO_ROOT / "src")
    environment["PYTHONPATH"] = os.pathsep.join(
        path for path in (source_path, environment.get("PYTHONPATH", "")) if path
    )
    return subprocess.run(
        [sys.executable, "-m", "job_scraper.main", *arguments],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def seed_job(database: Path) -> int:
    store = JobStore(database)
    job_id, _ = store.upsert_job(
        {
            "id": "system-job",
            "site": "indeed",
            "job_url": "https://example.com/system-job",
            "title": "Junior Software Engineer",
            "company": "Example",
            "location": "Remote",
            "role_family": "core_software",
            "seniority": "entry",
            "matched_terms": ["software engineer"],
        },
        query_group="core_software",
        search_term="software engineer",
    )
    return job_id


def test_cli_process_boundary_supports_review_and_export_workflow(tmp_path):
    database = tmp_path / "jobs.sqlite3"
    exports = tmp_path / "exports"
    job_id = seed_job(database)
    base = ("--database", str(database), "--output-dir", str(exports))

    listed = run_cli(*base, "list")
    assert listed.returncode == 0
    assert f"{job_id:>5}" in listed.stdout
    assert "Junior Software Engineer" in listed.stdout

    created = run_cli(
        *base, "application", "create", str(job_id), "--note", "Applied"
    )
    assert created.returncode == 0
    assert "Application created" in created.stdout

    status = run_cli(
        *base, "application", "status", str(job_id), "interviewing",
        "--note", "Good fit",
    )
    assert status.returncode == 0
    assert "marked interviewing" in status.stdout

    shown = run_cli(*base, "application", "show", str(job_id))
    assert shown.returncode == 0
    assert f"Job {job_id}: Junior Software Engineer" in shown.stdout
    assert "Application: interviewing" in shown.stdout

    exported = run_cli(*base, "export")
    assert exported.returncode == 0
    current = exports / "current-jobs.csv"
    with current.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    assert len(rows) == 1
    assert rows[0]["id"] == str(job_id)

    pipeline = run_cli(*base, "pipeline")
    assert pipeline.returncode == 0
    with (exports / "application-pipeline.csv").open(
        newline="", encoding="utf-8"
    ) as csv_file:
        pipeline_rows = list(csv.DictReader(csv_file))
    assert len(pipeline_rows) == 1
    assert pipeline_rows[0]["id"] == str(job_id)
    assert pipeline_rows[0]["application_status"] == "interviewing"
