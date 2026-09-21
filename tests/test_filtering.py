import pandas as pd
import pytest

from job_scraper import config
from job_scraper.filtering import classify_title, filter_jobs, normalize_text


def test_normalization_handles_hyphens_and_punctuation():
    assert normalize_text("  Full-Stack / Engineer! ") == "full stack engineer"


@pytest.mark.parametrize("term", config.EXCLUDED_TITLE_TERMS)
def test_classification_rejects_every_senior_title_term(term):
    result = classify_title(f"{term} Backend Engineer")
    assert not result.accepted
    assert result.reason.startswith("excluded:")


def test_classification_matches_hyphenated_and_entry_level_title():
    result = classify_title("Junior Full-Stack Engineer")
    assert result.accepted
    assert result.role_family == "backend_full_stack"
    assert result.seniority == "entry"


def test_short_terms_use_token_boundaries():
    assert not classify_title("HVAC Engineer").accepted
    assert classify_title("C++ Engineer").accepted
    assert classify_title("SRE").accepted
    assert not classify_title("Misreporting Analyst").accepted


def test_filter_jobs_reports_excluded_and_unmatched_counts():
    jobs = pd.DataFrame(
        [
            {"title": "Software Engineer", "job_url": "https://example.com/1"},
            {"title": "Staff Software Engineer", "job_url": "https://example.com/2"},
            {"title": "Marketing Analyst", "job_url": "https://example.com/3"},
        ]
    )
    accepted, stats = filter_jobs(jobs)
    assert accepted["title"].tolist() == ["Software Engineer"]
    assert stats.raw == 3
    assert stats.accepted == 1
    assert stats.excluded == 1
    assert stats.unmatched == 1
