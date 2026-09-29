from concurrent.futures import ThreadPoolExecutor
import logging
import multiprocessing
import time

import pandas as pd
import pytest

from job_scraper.sources import (
    JOBSPY_SOURCES,
    SourceRequest,
    _scrape_source_with_timeout_result,
    run_source_attempt,
    scrape_source,
    validate_sources,
)


def empty_scraper(**_kwargs):
    return pd.DataFrame()


@pytest.mark.parametrize("source", ["indeed", "linkedin"])
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
    assert JOBSPY_SOURCES == {"indeed", "linkedin"}
    validate_sources(["indeed", "linkedin"])


def test_glassdoor_is_rejected_as_an_unsupported_source():
    with pytest.raises(ValueError, match="Unsupported source.*glassdoor"):
        validate_sources(["glassdoor"])


def test_skillsire_is_not_a_live_source():
    with pytest.raises(ValueError, match="Unsupported source.*skillsire"):
        validate_sources(["skillsire"])


def test_source_attempt_reports_failures_with_request_context():
    def failing_scraper(**_kwargs):
        raise RuntimeError("blocked")

    result = run_source_attempt(
        SourceRequest("linkedin", "software engineer", 24),
        scraper=failing_scraper,
        timeout_seconds=0,
    )

    assert result.request.source == "linkedin"
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


def test_concurrent_no_timeout_error_capture_is_request_specific():
    def logging_scraper(**kwargs):
        term = kwargs["search_term"]
        logging.getLogger("jobspy-test").error("error-for-%s", term)
        time.sleep(0.05)
        return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(
            lambda term: run_source_attempt(
                SourceRequest("linkedin", term, 24),
                scraper=logging_scraper,
                timeout_seconds=0,
            ),
            ["one", "two"],
        ))

    assert [result.error for result in results] == [
        "error-for-one",
        "error-for-two",
    ]


def test_successful_timeout_worker_is_reaped():
    before = {process.pid for process in multiprocessing.active_children()}
    jobs, errors = _scrape_source_with_timeout_result(
        "indeed",
        "software engineer",
        24,
        empty_scraper,
        5,
    )

    assert jobs.empty
    assert errors == ()
    assert not {
        process.pid for process in multiprocessing.active_children()
        if process.pid not in before
    }


def test_unexpected_worker_error_still_cleans_up(monkeypatch):
    class FakeQueue:
        def __init__(self):
            self.closed = False
            self.joined = False

        def get(self, timeout):
            raise OSError("queue failed")

        def close(self):
            self.closed = True

        def join_thread(self):
            self.joined = True

    class FakeProcess:
        instance = None

        def __init__(self, **_kwargs):
            self.alive = True
            self.terminated = False
            self.joined = False
            FakeProcess.instance = self

        def start(self):
            return None

        def is_alive(self):
            return self.alive

        def terminate(self):
            self.terminated = True
            self.alive = False

        def kill(self):
            self.alive = False

        def join(self, _timeout):
            self.joined = True

    class FakeContext:
        def Queue(self, **_kwargs):
            queue = FakeQueue()
            self.queue = queue
            return queue

        def Process(self, **kwargs):
            return FakeProcess(**kwargs)

    context = FakeContext()
    monkeypatch.setattr(
        "job_scraper.sources.multiprocessing.get_context",
        lambda _method: context,
    )

    with pytest.raises(OSError, match="queue failed"):
        _scrape_source_with_timeout_result(
            "indeed", "software engineer", 24, empty_scraper, 5
        )

    assert FakeProcess.instance.terminated is True
    assert FakeProcess.instance.joined is True
    assert context.queue.closed is True
    assert context.queue.joined is True
