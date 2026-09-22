"""LinkedIn description fetching and GPT requirement analysis workers."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import math
import os
import re
import time
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from typing import Callable

import requests
from bs4 import BeautifulSoup

from . import config
from .queueing import EnrichmentQueue, normalize_description, prepare_description


LOGGER = logging.getLogger("job_scraper")


@dataclass
class WorkerStats:
    claimed: int = 0
    completed: int = 0
    retried: int = 0
    dead: int = 0
    budget_blocked: int = 0
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost: float = 0.0
    circuit_breaker: str = "closed"
    errors: list[str] = field(default_factory=list)


@dataclass
class EnrichmentStats:
    fetch: WorkerStats = field(default_factory=WorkerStats)
    analysis: WorkerStats = field(default_factory=WorkerStats)


class FetchError(RuntimeError):
    def __init__(self, message: str, *, permanent: bool = False,
                 retry_after: int | None = None, stop: bool = False):
        super().__init__(message)
        self.permanent = permanent
        self.retry_after = retry_after
        self.stop = stop


class AnalysisError(RuntimeError):
    def __init__(self, message: str, *, retry_after: int | None = None,
                 stop: bool = False, service_config: bool = False,
                 network: bool = False):
        super().__init__(message)
        self.retry_after = retry_after
        self.stop = stop
        self.service_config = service_config
        self.network = network


def _retry_after(headers: object) -> int | None:
    try:
        value = headers.get("Retry-After")
        if not value:
            return None
        try:
            return max(0, int(value))
        except ValueError:
            target = parsedate_to_datetime(value)
            return max(0, int((target - datetime.now(timezone.utc)).total_seconds()))
    except (AttributeError, TypeError, ValueError):
        return None


def fetch_linkedin_description(url: str) -> str:
    try:
        response = requests.get(
            url,
            timeout=config.DESCRIPTION_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0)"},
        )
    except (requests.Timeout, requests.ConnectionError) as error:
        raise FetchError(str(error)) from error
    if response.status_code in (404, 410):
        raise FetchError(f"LinkedIn returned HTTP {response.status_code}", permanent=True)
    if response.status_code == 429:
        raise FetchError(
            "LinkedIn rate limited the request", retry_after=_retry_after(response.headers) or 21600,
            stop=True,
        )
    if response.status_code >= 500:
        raise FetchError(f"LinkedIn returned HTTP {response.status_code}")
    if response.status_code >= 400:
        raise FetchError(f"LinkedIn returned HTTP {response.status_code}")
    if "login" in response.url.casefold() or "signin" in response.url.casefold():
        raise FetchError("LinkedIn redirected to sign-in")
    soup = BeautifulSoup(response.text, "html.parser")
    node = soup.select_one(
        ".show-more-less-html__markup, .description__text, [class*='job-description']"
    )
    description = node.get_text(" ", strip=True) if node else ""
    if not description:
        raise FetchError("LinkedIn description markup was missing or empty")
    return description


REQUIREMENTS_SCHEMA = {
    "type": "object",
    "properties": {
        "requirements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["skill", "experience"]},
                    "value": {
                        "type": "string",
                        "description": "Concise requirement name; for a skill use only the named competency or technology, normally 1-5 words.",
                    },
                    "category": {
                        "type": "string",
                        "description": "Short category such as language, framework, cloud, database, tooling, or experience.",
                    },
                    "priority": {"type": "string", "enum": ["required", "preferred"]},
                    "evidence": {"type": "string"},
                },
                "required": ["type", "value", "category", "priority", "evidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["requirements"],
    "additionalProperties": False,
}


SYSTEM_PROMPT = """Extract a complete inventory of explicit candidate skills and
numeric experience requirements.

Rules:
- Classify items under required/basic qualifications as required.
- Classify items under preferred qualifications, or described as preferred,
  "a plus," familiarity, or exposure, as preferred.
- Return every named technology or technical competency separately.
- Experience is valid only when the description explicitly states a numeric
  number or range of years.
- Copy a short verbatim evidence substring for every item.
- Keep skill values concise, normally 1-5 words.
- Exclude education, certifications, clearance, citizenship, work authorization,
  benefits, and general responsibilities.
- Do not infer unstated requirements."""


def call_openai(description: str) -> tuple[dict, int, int]:
    try:
        from openai import OpenAI
    except ImportError as error:
        raise AnalysisError(
            "OpenAI support is not installed; run `uv sync --extra llm`",
            service_config=True, stop=True,
        ) from error
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    try:
        response = client.responses.create(
            model=config.LLM_MODEL,
            reasoning={"effort": "low"},
            store=False,
            max_output_tokens=config.LLM_MAX_OUTPUT_TOKENS,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": description[:config.LLM_MAX_DESCRIPTION_CHARS]},
            ],
            text={"format": {
                "type": "json_schema", "name": "job_requirements", "strict": True,
                "schema": REQUIREMENTS_SCHEMA,
            }},
        )
    except Exception as error:
        status = getattr(error, "status_code", None)
        headers = getattr(getattr(error, "response", None), "headers", None)
        if status in (401, 403):
            raise AnalysisError(str(error), service_config=True, stop=True) from error
        if status == 429:
            raise AnalysisError(
                str(error), retry_after=_retry_after(headers) or 21600, stop=True,
                network=True,
            ) from error
        if status in (408,) or (status is not None and status >= 500):
            raise AnalysisError(str(error), network=True) from error
        if isinstance(error, (TimeoutError, ConnectionError)) or type(error).__name__ in {
            "APIConnectionError", "APITimeoutError"
        }:
            raise AnalysisError(str(error), network=True) from error
        raise AnalysisError(str(error)) from error
    try:
        payload = json.loads(response.output_text)
        usage = response.usage
        return payload, int(usage.input_tokens), int(usage.output_tokens)
    except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise AnalysisError(f"Invalid structured output: {error}") from error


ALIASES = {
    "amazon web services": "AWS", "aws": "AWS", "google cloud platform": "GCP",
    "gcp": "GCP", "javascript": "JavaScript", "typescript": "TypeScript",
    "node.js": "Node.js", "nodejs": "Node.js", "c sharp": "C#",
}
PRIORITY = {"preferred": 1, "required": 2}
ALLOWED_REQUIREMENT_TYPES = {"skill", "experience"}


def validate_requirements(payload: dict, description: str) -> list[dict]:
    if not isinstance(payload, dict) or not isinstance(payload.get("requirements"), list):
        raise AnalysisError("Structured output did not match the expected schema")
    def evidence_key(value: str) -> str:
        # Ignore visual punctuation variants introduced by HTML/model rendering,
        # while still requiring every evidence word to occur in source order.
        value = (value.replace("’", "'").replace("‘", "'")
                 .replace("–", "-").replace("—", "-"))
        return " ".join(re.sub(r"[^\w+#.]+", " ", value.casefold()).split())

    source = evidence_key(normalize_description(description))
    preferred_source = ""
    responsibility_source = ""
    description_lines = [line.strip() for line in description.splitlines()]
    preferred_index = next(
        (index for index, line in enumerate(description_lines)
         if re.fullmatch(r"[*_# ]*preferred (?:qualifications|skills|experience)[*_# :]*",
                         line, re.IGNORECASE)),
        None,
    )
    if preferred_index is not None:
        preferred_lines: list[str] = []
        for line in description_lines[preferred_index + 1:]:
            cleaned = re.sub(r"[*_#]", "", line).strip()
            if re.fullmatch(
                r"(?:screening note|benefits|pay|salary|work location|about the role|responsibilities)",
                cleaned, re.IGNORECASE,
            ):
                break
            preferred_lines.append(line)
        preferred_source = evidence_key(" ".join(preferred_lines))
    responsibility_index = next(
        (index for index, line in enumerate(description_lines)
         if re.fullmatch(r"[*_# ]*(?:key )?(?:responsibilities|duties|what you.ll do)[*_# :]*",
                         line, re.IGNORECASE)),
        None,
    )
    if responsibility_index is not None:
        responsibility_lines: list[str] = []
        for line in description_lines[responsibility_index + 1:]:
            cleaned = re.sub(r"[*_#]", "", line).strip()
            if re.fullmatch(
                r"(?:required skills|required qualifications|basic qualifications|"
                r"preferred qualifications|preferred skills)", cleaned, re.IGNORECASE,
            ):
                break
            responsibility_lines.append(line)
        responsibility_source = evidence_key(" ".join(responsibility_lines))
    accepted: dict[tuple[str, str], dict] = {}
    unsupported = 0
    for raw in payload["requirements"]:
        if not isinstance(raw, dict):
            raise AnalysisError("Structured output contained an invalid requirement")
        try:
            kind = raw["type"]
            value = normalize_description(raw["value"])
            priority = raw["priority"]
            evidence = normalize_description(raw["evidence"])
        except (KeyError, TypeError) as error:
            raise AnalysisError("Structured output omitted a required field") from error
        if kind not in ALLOWED_REQUIREMENT_TYPES or priority not in PRIORITY:
            unsupported += 1
            continue
        if kind == "skill" and re.search(r"\bcertificat(?:e|ion|ions)\b", value, re.I):
            unsupported += 1
            continue
        normalized_evidence = evidence_key(evidence)
        if (not value or not evidence or not normalized_evidence
                or normalized_evidence not in source
                or (kind == "skill" and len(value.split()) > 6)
                or (kind == "skill" and "," in value)
                or (responsibility_source and normalized_evidence in responsibility_source)):
            unsupported += 1
            continue
        if kind == "experience":
            years = re.search(r"\b\d+\s*(?:\+|[-–—]\s*\d+)?\s*(?:years?|yrs?)\b",
                              evidence, re.IGNORECASE)
            value_years = re.search(
                r"\b\d+\s*(?:\+|[-–—]\s*\d+)?\s*(?:years?|yrs?)\b",
                value, re.IGNORECASE,
            )
            if not years or not value_years or evidence_key(years.group()) != evidence_key(value_years.group()):
                unsupported += 1
                continue
        canonical = ALIASES.get(value.casefold(), value)
        if normalized_evidence in preferred_source or re.search(
            r"\b(preferred|a plus|nice to have|familiarity|exposure)\b",
            evidence.casefold(),
        ):
            priority = "preferred"
        key = (kind, canonical.casefold())
        item = {
            "type": kind, "value": canonical,
            "category": normalize_description(raw.get("category", "")),
            "priority": priority, "evidence": evidence,
        }
        if key not in accepted or PRIORITY[priority] > PRIORITY[accepted[key]["priority"]]:
            accepted[key] = item
    if payload["requirements"] and not accepted:
        raise AnalysisError(
            f"Evidence validation rejected all {unsupported} extracted requirements"
        )
    return sorted(accepted.values(), key=lambda x: (x["type"], x["value"].casefold()))


def validate_completeness(requirements: list[dict], description: str) -> None:
    """Reject conspicuously incomplete results from clearly structured sections."""
    lines = [re.sub(r"[*_#]", "", line).strip() for line in description.splitlines()]
    lines = [line for line in lines if line]
    values = " ".join(item["value"] for item in requirements if item["type"] == "skill")
    value_tokens = set(re.findall(r"[a-z0-9+#]+", values.casefold()))

    for index, line in enumerate(lines):
        if not re.search(r"\b(expertise|skills|technologies|tools)\s+with\s*:$", line, re.I):
            continue
        expected: list[str] = []
        for candidate in lines[index + 1:]:
            if len(candidate) > 45 or candidate.endswith(('.', ':')):
                break
            if len(candidate.split()) <= 4:
                expected.append(candidate)
        missing = []
        for candidate in expected:
            tokens = set(re.findall(r"[a-z0-9+#]+", candidate.casefold()))
            if not tokens.issubset(value_tokens):
                missing.append(candidate)
        if missing:
            raise AnalysisError(
                "Incomplete explicit skill list; missing: " + ", ".join(missing)
            )

    preferred_start = next(
        (i for i, line in enumerate(lines)
         if re.fullmatch(r"preferred (?:qualifications|skills|experience)", line, re.I)),
        None,
    )
    if preferred_start is not None:
        section: list[str] = []
        for line in lines[preferred_start + 1:]:
            if re.fullmatch(
                r"(?:screening note|benefits|pay|salary|work location|about the role|responsibilities)",
                line, re.I,
            ):
                break
            if not re.search(
                r"\b(certifications?|degree|education|clearance|citizenship|visa|work authorization)\b",
                line, re.I,
            ):
                section.append(line)
        preferred = [
            item for item in requirements
            if item["type"] == "skill" and item["priority"] == "preferred"
        ]
        if section and not preferred:
            raise AnalysisError(
                "Preferred Qualifications contains applicable content but no preferred skills"
            )


def supplement_structured_requirements(
    requirements: list[dict], description: str
) -> list[dict]:
    """Fill explicit structured lists that small models sometimes omit."""
    result = list(requirements)
    keys = {(item["type"], item["value"].casefold()) for item in result}

    def add_skill(value: str, priority: str, evidence: str) -> None:
        value = value.strip(" .:;*-_")
        if not value or len(value.split()) > 6:
            return
        key = ("skill", value.casefold())
        if key in keys:
            return
        result.append({
            "type": "skill", "value": value, "category": "technical",
            "priority": priority, "evidence": evidence,
        })
        keys.add(key)

    lines = [re.sub(r"[*_#]", "", line).strip() for line in description.splitlines()]
    lines = [line for line in lines if line]
    for index, line in enumerate(lines):
        if not re.search(r"\b(expertise|skills|technologies|tools)\s+with\s*:$", line, re.I):
            continue
        for candidate in lines[index + 1:]:
            if len(candidate) > 45 or candidate.endswith(('.', ':')):
                break
            if len(candidate.split()) > 4:
                continue
            parts = candidate.split("/") if re.fullmatch(r"[A-Z0-9+]+/[A-Z0-9+]+", candidate) else [candidate]
            for part in parts:
                add_skill(part, "required", candidate)

    preferred_start = next(
        (i for i, line in enumerate(lines)
         if re.fullmatch(r"preferred (?:qualifications|skills|experience)", line, re.I)),
        None,
    )
    if preferred_start is not None:
        for line in lines[preferred_start + 1:]:
            if re.fullmatch(
                r"(?:screening note|benefits|pay|salary|work location|about the role|responsibilities)",
                line, re.I,
            ):
                break
            if re.search(
                r"\b(certifications?|degree|education|clearance|citizenship|visa|work authorization)\b",
                line, re.I,
            ):
                continue
            value = re.sub(
                r"^(?:strong\s+)?(?:experience|understanding|knowledge|familiarity)\s+(?:with|of|in)\s+",
                "", line, flags=re.I,
            ).rstrip(".")
            for part in re.split(r",\s*(?:and\s+)?|\s+and\s+", value):
                add_skill(part, "preferred", line)
    return sorted(result, key=lambda item: (item["type"], item["value"].casefold()))


def projected_cost(description: str) -> float:
    input_tokens = math.ceil(len(description[:config.LLM_MAX_DESCRIPTION_CHARS]) / 3) + 500
    return (
        input_tokens * config.LLM_INPUT_PRICE_PER_MILLION
        + config.LLM_MAX_OUTPUT_TOKENS * config.LLM_OUTPUT_PRICE_PER_MILLION
    ) / 1_000_000


def _release_unprocessed(queue: EnrichmentQueue, tasks: list[dict], start: int,
                         reason: Exception) -> None:
    for task in tasks[start:]:
        queue.fail(task["id"], task["lease_token"], reason, preserve_attempt=True)


def process_fetch_queue(
    queue: EnrichmentQueue, *, current_run_id: int | None = None,
    limit: int = config.LINKEDIN_DESCRIPTION_LIMIT,
    fetcher: Callable[[str], str] = fetch_linkedin_description,
    sleeper: Callable[[float], None] = time.sleep,
    verbose_logging: bool | None = None,
) -> WorkerStats:
    stats = WorkerStats()
    verbose_logging = config.VERBOSE_LOGGING if verbose_logging is None else verbose_logging
    tasks = queue.claim("fetch_description", limit, current_run_id=current_run_id)
    stats.claimed = len(tasks)
    for index, task in enumerate(tasks):
        context = queue.task_context(task["id"])
        started = time.perf_counter()
        stop_after = False
        try:
            description = fetcher(context["url"])
            queue.complete_fetch(task["id"], task["lease_token"], description)
            stats.completed += 1
            outcome = "completed"
        except Exception as raw_error:
            error = raw_error if isinstance(raw_error, FetchError) else FetchError(str(raw_error))
            status = queue.fail(
                task["id"], task["lease_token"], error,
                permanent=error.permanent, retry_after=error.retry_after,
            )
            stats.dead += status == "dead"
            stats.retried += status == "retry"
            stats.errors.append(f"task {task['id']}: {error}")
            outcome = status
            if error.stop:
                stats.circuit_breaker = "open"
                _release_unprocessed(queue, tasks, index + 1, error)
                stop_after = True
        if verbose_logging:
            LOGGER.info(
                "enrichment: job_id=%s posting_id=%s task_id=%s stage=fetch outcome=%s duration_ms=%d title=%r",
                context["job_id"], context["posting_id"], task["id"], outcome,
                round((time.perf_counter() - started) * 1000), context["title"],
            )
        if stop_after:
            break
        if index < len(tasks) - 1:
            sleeper(config.QUERY_DELAY_SECONDS)
    return stats


def process_analysis_queue(
    queue: EnrichmentQueue, *, current_run_id: int | None = None,
    limit: int = config.LLM_MAX_CALLS_PER_CYCLE,
    analyzer: Callable[[str], tuple[dict, int, int]] = call_openai,
    enabled: bool | None = None,
    verbose_logging: bool | None = None,
) -> WorkerStats:
    stats = WorkerStats()
    verbose_logging = config.VERBOSE_LOGGING if verbose_logging is None else verbose_logging
    enabled = config.LLM_ENABLED if enabled is None else enabled
    if not enabled:
        return stats
    if analyzer is call_openai and not os.getenv("OPENAI_API_KEY"):
        stats.circuit_breaker = "configuration"
        stats.errors.append("OPENAI_API_KEY is not set")
        return stats
    tasks = queue.claim("analyze_description", limit, current_run_id=current_run_id)
    stats.claimed = len(tasks)
    consecutive_network_errors = 0
    for index, task in enumerate(tasks):
        context = queue.task_context(task["id"])
        description = prepare_description(context["description"] or "")
        projected = projected_cost(description)
        usage = queue.usage()
        if usage["cost"] + projected > config.LLM_MONTHLY_BUDGET_USD:
            queue.fail(task["id"], task["lease_token"], RuntimeError("Monthly budget exhausted"),
                       preserve_attempt=True)
            _release_unprocessed(queue, tasks, index + 1, RuntimeError("Monthly budget exhausted"))
            stats.budget_blocked += queue.mark_budget_blocked()
            stats.circuit_breaker = "budget"
            break
        reservation = queue.reserve_usage(task["id"], task["model"], projected)
        stats.calls += 1
        api_completed = False
        input_tokens = output_tokens = 0
        started = time.perf_counter()
        stop_after = False
        try:
            payload, input_tokens, output_tokens = analyzer(description)
            api_completed = True
            requirements = validate_requirements(payload, description)
            requirements = supplement_structured_requirements(requirements, description)
            validate_completeness(requirements, description)
            queue.complete_analysis(task["id"], task["lease_token"], requirements)
            queue.finish_usage(reservation, input_tokens=input_tokens, output_tokens=output_tokens)
            stats.completed += 1
            stats.input_tokens += input_tokens
            stats.output_tokens += output_tokens
            stats.estimated_cost += (
                input_tokens * config.LLM_INPUT_PRICE_PER_MILLION
                + output_tokens * config.LLM_OUTPUT_PRICE_PER_MILLION
            ) / 1_000_000
            consecutive_network_errors = 0
            outcome = "completed"
        except Exception as raw_error:
            error = raw_error if isinstance(raw_error, AnalysisError) else AnalysisError(str(raw_error))
            if api_completed:
                queue.finish_usage(
                    reservation, input_tokens=input_tokens, output_tokens=output_tokens
                )
                stats.input_tokens += input_tokens
                stats.output_tokens += output_tokens
                stats.estimated_cost += (
                    input_tokens * config.LLM_INPUT_PRICE_PER_MILLION
                    + output_tokens * config.LLM_OUTPUT_PRICE_PER_MILLION
                ) / 1_000_000
            else:
                queue.finish_usage(
                    reservation, status="unknown" if error.network else "failed"
                )
            status = queue.fail(
                task["id"], task["lease_token"], error,
                retry_after=error.retry_after, preserve_attempt=error.service_config,
            )
            stats.dead += status == "dead"
            stats.retried += status == "retry"
            stats.errors.append(f"task {task['id']}: {error}")
            outcome = status
            consecutive_network_errors = consecutive_network_errors + 1 if error.network else 0
            if error.stop or consecutive_network_errors >= 2:
                stats.circuit_breaker = "configuration" if error.service_config else "open"
                _release_unprocessed(queue, tasks, index + 1, error)
                stop_after = True
        if verbose_logging:
            LOGGER.info(
                "enrichment: job_id=%s posting_id=%s task_id=%s stage=analysis outcome=%s duration_ms=%d title=%r",
                context["job_id"], context["posting_id"], task["id"], outcome,
                round((time.perf_counter() - started) * 1000), context["title"],
            )
        if stop_after:
            break
    return stats


def process_enrichment(
    queue: EnrichmentQueue, *, current_run_id: int | None = None,
    fetch_limit: int = config.LINKEDIN_DESCRIPTION_LIMIT,
    analysis_limit: int = config.LLM_MAX_CALLS_PER_CYCLE,
    fetch_only: bool = False,
    verbose_logging: bool | None = None,
) -> EnrichmentStats:
    verbose_logging = config.VERBOSE_LOGGING if verbose_logging is None else verbose_logging
    fetch = process_fetch_queue(
        queue, current_run_id=current_run_id, limit=fetch_limit,
        verbose_logging=verbose_logging,
    )
    analysis = WorkerStats() if fetch_only else process_analysis_queue(
        queue, current_run_id=current_run_id, limit=analysis_limit,
        verbose_logging=verbose_logging,
    )
    return EnrichmentStats(fetch=fetch, analysis=analysis)
