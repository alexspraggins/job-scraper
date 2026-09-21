"""Deterministic title normalization, matching, and classification."""

from dataclasses import dataclass
import re
import unicodedata

import pandas as pd

from . import config


LEVEL_ONE_PATTERN = re.compile(r"(?<![a-z0-9])(?:i|1)(?![a-z0-9])")
HIGHER_LEVEL_PATTERN = re.compile(
    r"(?<![a-z0-9])(?:ii|iii|iv|v|vi|vii|viii|ix|x|[2-9]|10)(?![a-z0-9])"
)


@dataclass(frozen=True)
class FilterStats:
    raw: int
    accepted: int
    excluded: int
    unmatched: int


@dataclass(frozen=True)
class TitleMatch:
    accepted: bool
    reason: str
    role_family: str | None
    matched_terms: tuple[str, ...]
    seniority: str


def normalize_text(value: object) -> str:
    """Normalize text while retaining plus signs used in C++ titles."""
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = re.sub(r"[\-‐‑‒–—―_/]", " ", text)
    text = re.sub(r"[^\w+\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def phrase_matches(normalized_title: str, phrase: str) -> bool:
    normalized_phrase = normalize_text(phrase)
    if not normalized_phrase:
        return False
    pattern = rf"(?<![a-z0-9]){re.escape(normalized_phrase)}(?![a-z0-9])"
    return re.search(pattern, normalized_title) is not None


def classify_title(
    title: object,
    role_terms: dict[str, list[str]] | None = None,
    excluded_terms: list[str] | None = None,
) -> TitleMatch:
    role_terms = role_terms or config.ROLE_TERMS
    excluded_terms = excluded_terms or config.EXCLUDED_TITLE_TERMS
    normalized_title = normalize_text(title)

    for term in excluded_terms:
        if phrase_matches(normalized_title, term):
            return TitleMatch(False, f"excluded:{normalize_text(term)}", None, (), "excluded")

    if HIGHER_LEVEL_PATTERN.search(normalized_title):
        return TitleMatch(False, "excluded:level 2+", None, (), "excluded")

    matches: list[tuple[str, str]] = []
    for family, terms in role_terms.items():
        for term in terms:
            if phrase_matches(normalized_title, term):
                matches.append((family, term))

    if not matches:
        return TitleMatch(False, "unmatched", None, (), "unknown")

    has_entry_marker = LEVEL_ONE_PATTERN.search(normalized_title) is not None or any(
        phrase_matches(normalized_title, term) for term in config.ENTRY_LEVEL_TERMS
    )
    seniority = "entry" if has_entry_marker else "unspecified"
    primary_family = matches[0][0]
    matched_terms = tuple(dict.fromkeys(term for _, term in matches))
    return TitleMatch(True, "accepted", primary_family, matched_terms, seniority)


def filter_jobs(
    current_jobs: pd.DataFrame,
    role_terms: dict[str, list[str]] | None = None,
    excluded_terms: list[str] | None = None,
) -> tuple[pd.DataFrame, FilterStats]:
    """Filter and annotate scraped jobs, returning accepted rows and counts."""
    if current_jobs.empty:
        return current_jobs.copy(), FilterStats(0, 0, 0, 0)

    accepted_rows: list[dict] = []
    excluded = 0
    unmatched = 0

    for row in current_jobs.to_dict(orient="records"):
        decision = classify_title(row.get("title"), role_terms, excluded_terms)
        if not decision.accepted:
            if decision.reason.startswith("excluded:"):
                excluded += 1
            else:
                unmatched += 1
            continue

        row["role_family"] = decision.role_family
        row["matched_terms"] = list(decision.matched_terms)
        row["seniority"] = decision.seniority
        accepted_rows.append(row)

    accepted = pd.DataFrame(accepted_rows)
    stats = FilterStats(len(current_jobs), len(accepted_rows), excluded, unmatched)
    return accepted, stats
