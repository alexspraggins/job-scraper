import pandas as pd
import pytest

from job_scraper import config
from job_scraper.filtering import classify_title, filter_jobs, normalize_text


def test_normalization_handles_hyphens_and_punctuation():
    assert normalize_text("  Full-Stack / Engineer! ") == "full stack engineer"


@pytest.mark.parametrize("term", config.EXCLUDED_TITLE_TERMS)
def test_classification_rejects_every_senior_title_term(term):
    result = classify_title(f"{term} Junior Backend Engineer")
    assert not result.accepted
    assert result.reason.startswith("excluded:")


def test_classification_matches_hyphenated_and_entry_level_title():
    result = classify_title("Junior Full-Stack Engineer")
    assert result.accepted
    assert result.role_family == "backend_full_stack"
    assert result.seniority == "entry"


def test_short_terms_use_token_boundaries():
    assert not classify_title("HVAC Engineer").accepted
    assert classify_title("C++ Engineer I").accepted
    assert classify_title("SRE I").accepted
    assert not classify_title("Misreporting Analyst").accepted


@pytest.mark.parametrize(
    "title",
    [
        "Software Engineer II",
        "Software Engineer III",
        "Software Engineer IV",
        "Software Engineer 2",
        "Software Developer 3",
        "Junior Software Engineer II",
    ],
)
def test_level_two_and_higher_titles_are_rejected(title):
    result = classify_title(title)
    assert not result.accepted
    assert result.reason == "excluded:level 2+"


def test_unleveled_title_is_accepted_without_entry_marker():
    result = classify_title("Software Engineer")
    assert result.accepted
    assert result.seniority == "unspecified"


@pytest.mark.parametrize(
    "title",
    [
        "Mid-Level Software Engineer",
        "Midlevel Software Developer",
        "Intermediate Backend Engineer",
        "Experienced Software Engineer",
    ],
)
def test_non_entry_career_stage_titles_are_rejected(title):
    result = classify_title(title)
    assert not result.accepted
    assert result.reason.startswith("excluded:")


@pytest.mark.parametrize(
    "title",
    [
        "Software Engineer I",
        "Software Engineer 1",
        "Entry Software Engineer",
        "Entry Level Software Engineer",
        "Associate Software Developer",
        "New Grad Software Engineer",
        "Graduate Software Engineer",
        "Early Career Software Engineer",
        "Software Engineer Apprentice",
    ],
)
def test_explicit_entry_level_titles_are_accepted(title):
    result = classify_title(title)
    assert result.accepted
    assert result.seniority == "entry"


def test_filter_jobs_reports_excluded_and_unmatched_counts():
    jobs = pd.DataFrame(
        [
            {"title": "Software Engineer I", "job_url": "https://example.com/1"},
            {"title": "Staff Software Engineer", "job_url": "https://example.com/2"},
            {"title": "Marketing Analyst", "job_url": "https://example.com/3"},
        ]
    )
    accepted, stats = filter_jobs(jobs)
    assert accepted["title"].tolist() == ["Software Engineer I"]
    assert stats.raw == 3
    assert stats.accepted == 1
    assert stats.excluded == 1
    assert stats.unmatched == 1
