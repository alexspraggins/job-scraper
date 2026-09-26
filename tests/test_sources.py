import logging

import pandas as pd
import pytest

from job_scraper.sources import (
    JOBSPY_SOURCES,
    SourceRequest,
    run_source_attempt,
    scrape_source,
    validate_sources,
)


@pytest.mark.parametrize("source", ["indeed", "linkedin", "glassdoor"])
def test_jobspy_adapter_uses_one_common_request_contract(source):
    captured = {}

    def fake_scraper(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame([{"site": source, "title": "Software Engineer"}])

    result = scrape_source(source, "software engineer", 24, fake_scraper)

    assert captured == {
        "site_name": [source],
        "search_term": "software engineer",
        "location": "United States",
        "results_wanted": 15,
        "hours_old": 24,
        "country_indeed": "USA",
    }
    assert list(result["title"]) == ["Software Engineer"]


def test_source_registry_contains_default_jobspy_sources():
    assert JOBSPY_SOURCES == {"indeed", "linkedin", "glassdoor"}
    validate_sources(["indeed", "linkedin", "glassdoor"])


def test_skillsire_is_not_a_live_source():
    with pytest.raises(ValueError, match="Unsupported source.*skillsire"):
        validate_sources(["skillsire"])


def test_source_attempt_reports_failures_with_request_context():
    def failing_scraper(**_kwargs):
        raise RuntimeError("blocked")

    result = run_source_attempt(
        SourceRequest("glassdoor", "software engineer", 24),
        scraper=failing_scraper,
        timeout_seconds=0,
    )

    assert result.request.source == "glassdoor"
    assert result.request.search_term == "software engineer"
    assert result.jobs.empty
    assert result.error == "RuntimeError: blocked"
    assert result.duration_ms >= 0


def test_source_attempt_promotes_jobspy_error_logs():
    def logging_scraper(**_kwargs):
        logging.getLogger("jobspy-test").error("DNS unavailable")
        return pd.DataFrame()

    result = run_source_attempt(
        SourceRequest("linkedin", "software engineer", 24),
        scraper=logging_scraper,
        timeout_seconds=0,
    )

    assert result.error == "DNS unavailable"
    assert result.jobs.empty
