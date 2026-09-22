import logging

from job_scraper import config
from job_scraper.enrichment import (
    AnalysisError,
    FetchError,
    process_analysis_queue,
    process_fetch_queue,
    supplement_structured_requirements,
    validate_completeness,
    validate_requirements,
)
from job_scraper.queueing import EnrichmentQueue, normalize_description, prepare_description
from job_scraper.storage import JobStore


def add_posting(store, source="indeed", description=None, suffix="1"):
    job_id, _ = store.upsert_job(
        {
            "id": f"{source}-{suffix}",
            "site": source,
            "job_url": f"https://www.linkedin.com/jobs/view/{suffix}" if source == "linkedin"
                else f"https://example.com/{suffix}",
            "title": "Software Engineer",
            "company": f"Example {suffix}",
            "location": "Remote",
            "description": description,
            "role_family": "core_software",
            "seniority": "unspecified",
            "matched_terms": ["software engineer"],
        },
        query_group="core_software",
        search_term="software engineer",
    )
    return job_id


def test_description_normalization_removes_markdown_punctuation_escapes():
    assert normalize_description(r"Requires 2\+ years of non\-internship experience") == (
        "Requires 2+ years of non-internship experience"
    )


def test_description_preparation_preserves_section_boundaries():
    source = "Required Skills  \n**AWS Lambda**  \n\nPreferred Qualifications  \nGraphQL"
    assert prepare_description(source) == (
        "Required Skills\n**AWS Lambda**\n\nPreferred Qualifications\nGraphQL"
    )


def test_queue_creation_is_idempotent_and_description_change_cancels_old_task(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, description="Python is required.")

    run_id = store.start_run(2)
    first = queue.sync_job(job_id, run_id)
    second = queue.sync_job(job_id, run_id)
    assert first["analysis_created"] == 1
    assert second["analysis_created"] == 0

    add_posting(store, description="Java is required.")
    changed = queue.sync_job(job_id, run_id)
    assert changed["analysis_created"] == 1
    with store.connect() as connection:
        statuses = connection.execute(
            "SELECT status FROM enrichment_tasks ORDER BY id"
        ).fetchall()
    assert [row[0] for row in statuses] == ["cancelled", "pending"]


def test_existing_job_migration_can_be_deferred_until_explicit_repair(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_id = add_posting(store, description="Python is required.")

    queue = EnrichmentQueue(store, migrate_existing=False)
    assert queue.queue_summary() == []

    queue.migrate_existing()
    assert queue.list_tasks("pending")[0]["job_id"] == job_id


def test_linkedin_fetch_completion_creates_analysis_in_same_operation(tmp_path, caplog):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, source="linkedin")
    queue.sync_job(job_id)

    with caplog.at_level(logging.INFO, logger="job_scraper"):
        stats = process_fetch_queue(
            queue, limit=1, fetcher=lambda url: "Requires Python and SQL.",
            sleeper=lambda _: None, verbose_logging=True,
        )
    assert stats.completed == 1
    with store.connect() as connection:
        posting = connection.execute(
            "SELECT description,description_hash FROM postings WHERE job_id=?", (job_id,)
        ).fetchone()
        tasks = connection.execute(
            "SELECT task_type,status FROM enrichment_tasks ORDER BY id"
        ).fetchall()
    assert posting["description"] == "Requires Python and SQL."
    assert posting["description_hash"]
    assert [tuple(row) for row in tasks] == [
        ("fetch_description", "completed"), ("analyze_description", "pending")
    ]
    assert any(
        "stage=fetch" in record.message
        and f"job_id={job_id}" in record.message
        and "task_id=" in record.message
        and "duration_ms=" in record.message
        and "outcome=completed" in record.message
        for record in caplog.records
    )


def test_three_item_failures_dead_letter_task(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, source="linkedin")
    queue.sync_job(job_id)

    for expected in ("retry", "retry", "dead"):
        with store.connect() as connection:
            connection.execute(
                "UPDATE enrichment_tasks SET next_attempt_at='2000-01-01T00:00:00+00:00'"
            )
        task = queue.claim("fetch_description", 1)[0]
        assert queue.fail(task["id"], task["lease_token"], FetchError("temporary")) == expected
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM enrichment_task_events WHERE task_id=?", (task["id"],)
        ).fetchone()[0] == 3
    assert queue.retry(task_id=task["id"]) == 1
    assert queue.list_tasks("pending")[0]["attempt_count"] == 0


def test_fairness_reserves_three_slots_for_backlog(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    current_run = store.start_run(2)
    for number in range(10):
        job_id = add_posting(store, description="Python required", suffix=f"new-{number}")
        queue.sync_job(job_id, current_run)
    for number in range(5):
        job_id = add_posting(store, description="SQL required", suffix=f"old-{number}")
        queue.sync_job(job_id, None)

    tasks = queue.claim("analyze_description", 10, current_run_id=current_run)
    assert sum(task["scrape_run_id"] == current_run for task in tasks) == 7
    assert sum(task["scrape_run_id"] is None for task in tasks) == 3


def test_analysis_validates_evidence_and_stores_requirements(tmp_path, caplog):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, description="Python is required. AWS is preferred.")
    queue.sync_job(job_id)

    def analyzer(_description):
        return ({"requirements": [
            {"type": "skill", "value": "python", "category": "language",
             "priority": "required", "evidence": "Python is required"},
            {"type": "skill", "value": "amazon web services", "category": "cloud",
             "priority": "preferred", "evidence": "AWS is preferred"},
        ]}, 50, 20)

    with caplog.at_level(logging.INFO, logger="job_scraper"):
        stats = process_analysis_queue(
            queue, limit=1, analyzer=analyzer, enabled=True, verbose_logging=True
        )
    assert stats.completed == 1
    details = store.get_job_details(job_id)
    assert {item["canonical_value"] for item in details["requirements"]} == {"python", "AWS"}
    assert queue.usage()["calls"] == 1
    assert any(
        "stage=analysis" in record.message
        and f"job_id={job_id}" in record.message
        and "task_id=" in record.message
        and "duration_ms=" in record.message
        and "outcome=completed" in record.message
        for record in caplog.records
    )


def test_invalid_evidence_is_retried(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, description="Python is required.")
    queue.sync_job(job_id)

    def analyzer(_description):
        return ({"requirements": [{"type": "skill", "value": "Rust", "category": "language",
            "priority": "required", "evidence": "Rust is required"}]}, 10, 10)

    stats = process_analysis_queue(queue, limit=1, analyzer=analyzer, enabled=True)
    assert stats.retried == 1
    assert queue.list_tasks("retry")[0]["attempt_count"] == 1


def test_invalid_entries_are_dropped_when_supported_entries_remain(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, description="Python is required.")
    queue.sync_job(job_id)

    def analyzer(_description):
        return ({"requirements": [
            {"type": "skill", "value": "Python", "category": "language",
             "priority": "required", "evidence": "Python is required"},
            {"type": "skill", "value": "Rust", "category": "language",
             "priority": "required", "evidence": "Rust is required"},
        ]}, 10, 10)

    stats = process_analysis_queue(queue, limit=1, analyzer=analyzer, enabled=True)
    assert stats.completed == 1
    requirements = store.get_job_details(job_id)["requirements"]
    assert [item["canonical_value"] for item in requirements] == ["Python"]


def test_preferred_language_overrides_incorrect_required_priority():
    payload = {"requirements": [{
        "type": "skill", "value": "PySpark", "category": "framework",
        "priority": "required", "evidence": "PySpark experience is a plus",
    }]}
    result = validate_requirements(payload, "PySpark experience is a plus")
    assert result[0]["priority"] == "preferred"


def test_preferred_section_location_overrides_incorrect_priority():
    payload = {"requirements": [{
        "type": "skill", "value": "Cloud security", "category": "cloud",
        "priority": "required", "evidence": "Strong understanding of cloud security",
    }]}
    description = (
        "Required Skills\nPython\n\nPreferred Qualifications\n"
        "Strong understanding of cloud security\nScreening Note\nAWS is critical"
    )
    result = validate_requirements(payload, description)
    assert result[0]["priority"] == "preferred"


def test_experience_requires_matching_explicit_year_evidence():
    payload = {"requirements": [
        {"type": "skill", "value": "Python", "category": "language",
         "priority": "required", "evidence": "Strong Python experience"},
        {"type": "experience", "value": "3+ years Python", "category": "experience",
         "priority": "required", "evidence": "Strong Python experience"},
    ]}
    result = validate_requirements(payload, "Strong Python experience")
    assert [(item["type"], item["value"]) for item in result] == [("skill", "Python")]


def test_certification_named_values_are_not_stored_as_skills():
    payload = {"requirements": [
        {"type": "skill", "value": "AWS Certification", "category": "cloud",
         "priority": "preferred", "evidence": "AWS Certification preferred"},
        {"type": "skill", "value": "AWS", "category": "cloud",
         "priority": "required", "evidence": "AWS experience required"},
    ]}
    result = validate_requirements(
        payload, "AWS experience required. AWS Certification preferred"
    )
    assert [item["value"] for item in result] == ["AWS"]


def test_responsibility_items_and_combined_skill_values_are_rejected():
    payload = {"requirements": [
        {"type": "skill", "value": "DevOps collaboration", "category": "process",
         "priority": "required", "evidence": "Collaborate with DevOps teams"},
        {"type": "skill", "value": "Cloud security, scalability", "category": "cloud",
         "priority": "preferred", "evidence": "Cloud security and scalability preferred"},
        {"type": "skill", "value": "Python", "category": "language",
         "priority": "required", "evidence": "Python is required"},
    ]}
    description = (
        "Key Responsibilities\nCollaborate with DevOps teams\n"
        "Required Skills\nPython is required\n"
        "Preferred Qualifications\nCloud security and scalability preferred"
    )
    result = validate_requirements(payload, description)
    assert [item["value"] for item in result] == ["Python"]


def test_completeness_rejects_missing_explicit_list_items():
    description = "Required Skills\nHands-on expertise with:\nAWS Lambda\nDynamoDB\nRequired prose."
    requirements = [{"type": "skill", "value": "AWS Lambda", "priority": "required"}]
    try:
        validate_completeness(requirements, description)
    except AnalysisError as error:
        assert "DynamoDB" in str(error)
    else:
        raise AssertionError("missing explicit list item should fail completeness")


def test_completeness_rejects_empty_substantive_preferred_section():
    description = "Preferred Qualifications\nCloud security and scalability\nScreening Note"
    try:
        validate_completeness([], description)
    except AnalysisError as error:
        assert "no preferred skills" in str(error)
    else:
        raise AssertionError("empty preferred extraction should fail completeness")


def test_structured_supplement_fills_lists_and_preferred_competencies():
    description = """Required Skills
Hands-on expertise with:
AWS Lambda
API Gateway
ECS/EKS
Required prose.

Preferred Qualifications
Experience with enterprise-scale applications.
Strong understanding of cloud security, scalability, and performance optimization.
AWS Certifications preferred.
Screening Note
AWS is critical.
"""
    result = supplement_structured_requirements([], description)
    values = {(item["value"], item["priority"]) for item in result}
    assert ("AWS Lambda", "required") in values
    assert ("API Gateway", "required") in values
    assert ("ECS", "required") in values
    assert ("EKS", "required") in values
    assert ("enterprise-scale applications", "preferred") in values
    assert ("cloud security", "preferred") in values
    assert ("scalability", "preferred") in values
    assert ("performance optimization", "preferred") in values
    assert not any("Certification" in value for value, _ in values)


def test_budget_blocks_without_consuming_attempt(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, description="Python is required.")
    queue.sync_job(job_id)
    monkeypatch.setattr(config, "LLM_MONTHLY_BUDGET_USD", 0.0)

    stats = process_analysis_queue(queue, limit=1, analyzer=lambda _: ({}, 0, 0), enabled=True)
    task = queue.list_tasks("budget_blocked")[0]
    assert stats.budget_blocked == 1
    assert task["attempt_count"] == 0


def test_rejected_job_cancels_unfinished_tasks(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, description="Python is required.")
    queue.sync_job(job_id)
    store.set_status(job_id, "rejected")
    assert queue.list_tasks("cancelled")
    store.set_status(job_id, "saved")
    queue.sync_job(job_id)
    assert queue.list_tasks("pending")


def test_expired_lease_is_counted_and_recovered_as_retry(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    job_id = add_posting(store, source="linkedin")
    queue.sync_job(job_id)
    task = queue.claim("fetch_description", 1)[0]
    with store.connect() as connection:
        connection.execute(
            "UPDATE enrichment_tasks SET lease_expires_at='2000-01-01T00:00:00+00:00' WHERE id=?",
            (task["id"],),
        )

    assert queue.claim("fetch_description", 1) == []
    recovered = queue.list_tasks("retry")[0]
    assert recovered["attempt_count"] == 1
    assert recovered["last_error_class"] == "LeaseExpired"


def test_linkedin_429_stops_cycle_and_releases_unstarted_tasks(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    for suffix in ("1", "2"):
        queue.sync_job(add_posting(store, source="linkedin", suffix=suffix))

    def rate_limited(_url):
        raise FetchError("rate limited", retry_after=10, stop=True)

    stats = process_fetch_queue(queue, limit=2, fetcher=rate_limited, sleeper=lambda _: None)
    assert stats.circuit_breaker == "open"
    assert stats.retried == 1
    assert queue.list_tasks("pending")[0]["attempt_count"] == 0


def test_openai_configuration_error_does_not_consume_item_attempt(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    queue = EnrichmentQueue(store)
    queue.sync_job(add_posting(store, description="Python required"))

    def auth_failure(_description):
        raise AnalysisError("bad API key", service_config=True, stop=True)

    stats = process_analysis_queue(queue, limit=1, analyzer=auth_failure, enabled=True)
    assert stats.circuit_breaker == "configuration"
    assert queue.list_tasks("pending")[0]["attempt_count"] == 0
    assert queue.usage()["cost"] == 0


def test_existing_data_migration_queues_both_sources(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    add_posting(store, description="Python required", suffix="indeed")
    add_posting(store, source="linkedin", suffix="linkedin")
    queue = EnrichmentQueue(store)
    summary = {(row["task_type"], row["status"]): row["count"] for row in queue.queue_summary()}
    assert summary[("analyze_description", "pending")] == 1
    assert summary[("fetch_description", "pending")] == 1


def test_validate_requirements_resolves_priority_conflicts():
    payload = {"requirements": [
        {"type": "skill", "value": "AWS", "category": "cloud",
         "priority": "preferred", "evidence": "AWS"},
        {"type": "skill", "value": "Amazon Web Services", "category": "cloud",
         "priority": "required", "evidence": "Amazon Web Services"},
    ]}
    result = validate_requirements(payload, "AWS / Amazon Web Services")
    assert len(result) == 1
    assert result[0]["priority"] == "required"


def test_punctuation_only_evidence_is_rejected():
    payload = {"requirements": [{
        "type": "experience", "value": "3+ years Python", "category": "experience",
        "priority": "required", "evidence": "-",
    }]}
    try:
        validate_requirements(payload, "Three years of Python is required")
    except AnalysisError as error:
        assert "rejected all" in str(error)
    else:
        raise AssertionError("punctuation-only evidence should fail validation")
