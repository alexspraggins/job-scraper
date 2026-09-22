"""Application configuration for the job scraper."""

import os
from pathlib import Path

from dotenv import load_dotenv


# Load local development settings without overriding values explicitly exported
# by the shell or supplied by a process manager.
load_dotenv(override=False)


SEARCH_GROUPS: dict[str, list[str]] = {
    "core_software": [
        "software engineer",
        "software developer",
    ],
    "backend_full_stack": [
        "backend engineer",
        "full stack developer",
    ],
    "language_specific": [
        "python developer",
        "java developer",
    ],
    "embedded_systems": [
        "embedded software engineer",
        "firmware engineer",
    ],
    "platform_cloud": [
        "platform engineer",
    ],
    "data": [
        "data engineer",
    ],
    "quality_automation": [
        "test automation engineer",
    ],
    "robotics_edge": [
        "robotics software engineer",
        "computer vision engineer",
    ],
    "integration_research": [
        "integration engineer",
        "research software engineer",
    ],
}

ROLE_TERMS: dict[str, list[str]] = {
    "core_software": [
        "software engineer",
        "software developer",
        "software development engineer",
    ],
    "backend_full_stack": [
        "backend engineer",
        "backend software engineer",
        "backend developer",
        "back end engineer",
        "back end developer",
        "api developer",
        "api engineer",
        "server side engineer",
        "full stack engineer",
        "full stack developer",
        "fullstack engineer",
        "fullstack developer",
        "application developer",
        "application engineer",
    ],
    "language_specific": [
        "python developer",
        "python engineer",
        "python software engineer",
        "java developer",
        "java engineer",
        "java software engineer",
        "c++ developer",
        "c++ engineer",
        "c++ software engineer",
        "c developer",
        "c engineer",
        "go developer",
        "go engineer",
        "golang developer",
        "golang engineer",
    ],
    "embedded_systems": [
        "embedded software engineer",
        "embedded software developer",
        "embedded engineer",
        "embedded systems engineer",
        "embedded systems developer",
        "embedded c engineer",
        "embedded c++ engineer",
        "firmware engineer",
        "firmware developer",
        "firmware software engineer",
        "device software engineer",
        "systems software engineer",
        "systems engineer",
        "systems programmer",
        "systems developer",
        "low level software engineer",
        "linux software engineer",
        "linux engineer",
    ],
    "platform_cloud": [
        "platform engineer",
        "platform software engineer",
        "infrastructure engineer",
        "cloud engineer",
        "cloud software engineer",
        "devops engineer",
        "site reliability engineer",
        "sre",
    ],
    "data": [
        "data engineer",
        "data platform engineer",
        "etl developer",
        "etl engineer",
        "analytics engineer",
    ],
    "quality_automation": [
        "automation engineer",
        "software automation engineer",
        "test automation engineer",
        "software test engineer",
        "software qa engineer",
        "qa automation engineer",
        "sdet",
        "software development engineer in test",
        "verification engineer",
        "validation engineer",
        "software validation engineer",
    ],
    "robotics_edge": [
        "robotics software engineer",
        "robotics engineer",
        "controls software engineer",
        "controls engineer",
        "autonomy engineer",
        "computer vision engineer",
        "iot engineer",
        "iot software engineer",
        "edge software engineer",
        "edge computing engineer",
    ],
    "integration_research": [
        "integration engineer",
        "software integration engineer",
        "systems integration engineer",
        "solutions engineer",
        "implementation engineer",
        "technical solutions engineer",
        "research software engineer",
        "scientific software engineer",
        "simulation software engineer",
        "modeling and simulation engineer",
    ],
}

EXCLUDED_TITLE_TERMS = [
    "mid level",
    "midlevel",
    "intermediate",
    "experienced",
    "senior",
    "sr",
    "staff",
    "principal",
    "lead",
    "manager",
    "director",
    "architect",
    "distinguished",
    "vice president",
]

ENTRY_LEVEL_TERMS = [
    "entry",
    "entry level",
    "early career",
    "junior",
    "associate",
    "new grad",
    "graduate",
    "recent graduate",
    "college graduate",
    "university graduate",
    "apprentice",
    "level 1",
    "level i",
    "engineer 1",
    "engineer i",
    "developer 1",
    "developer i",
]

SOURCES = ["indeed", "linkedin"]
SKILLSIRE_ENABLED = False
COUNTRY = "USA"
LOCATION = "United States"
RESULTS_PER_QUERY = 15
INITIAL_LOOKBACK_HOURS = 24
RECURRING_LOOKBACK_HOURS = 2
QUERY_DELAY_SECONDS = 2
SOURCE_TIMEOUT_SECONDS = 45
# Indeed queries run in a small parallel pool while LinkedIn stays serial. This
# overlaps the two sources without increasing LinkedIn's request rate.
INDEED_MAX_WORKERS = max(1, int(os.getenv("JOB_SCRAPER_INDEED_MAX_WORKERS", "3")))
POLL_INTERVAL_SECONDS = 3600

DATA_DIR = Path("data")
DATABASE_PATH = DATA_DIR / "jobs.sqlite3"
EXPORT_DIR = DATA_DIR / "exports"

EMAIL_ENABLED = os.getenv("JOB_SCRAPER_EMAIL_ENABLED", "").lower() in {
    "1",
    "true",
    "yes",
}
EMAIL_FROM = os.getenv("JOB_SCRAPER_EMAIL_FROM", "")
EMAIL_PASSWORD = os.getenv("JOB_SCRAPER_EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("JOB_SCRAPER_EMAIL_TO", "")
EMAIL_SMTP = os.getenv("JOB_SCRAPER_EMAIL_SMTP", "")

# Description enrichment and OpenAI analysis. LLM processing is opt-in so a
# normal scrape never incurs API charges unexpectedly.
LINKEDIN_DESCRIPTION_LIMIT = int(
    os.getenv("JOB_SCRAPER_LINKEDIN_DESCRIPTION_LIMIT", "10")
)
DESCRIPTION_FETCH_TIMEOUT_SECONDS = 20
ENRICHMENT_LEASE_SECONDS = 15 * 60
ENRICHMENT_NEW_TASK_QUOTA = 7
ENRICHMENT_BACKLOG_TASK_QUOTA = 3
ENRICHMENT_MAX_ATTEMPTS = 3

LLM_ENABLED = os.getenv("JOB_SCRAPER_LLM_ENABLED", "false").lower() in {
    "1", "true", "yes"
}
LLM_MODEL = os.getenv("JOB_SCRAPER_LLM_MODEL", "gpt-5-nano")
LLM_MONTHLY_BUDGET_USD = float(
    os.getenv("JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD", "3.00")
)
LLM_MAX_CALLS_PER_CYCLE = int(
    os.getenv("JOB_SCRAPER_LLM_MAX_CALLS_PER_CYCLE", "10")
)
LLM_PROMPT_VERSION = "11"
LLM_MAX_DESCRIPTION_CHARS = 30_000
LLM_MAX_OUTPUT_TOKENS = int(
    os.getenv("JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS", "6000")
)
VERBOSE_LOGGING = os.getenv("JOB_SCRAPER_VERBOSE_LOGGING", "false").lower() in {
    "1", "true", "yes"
}
# USD per one million tokens. Keep these conservative and versioned in usage rows.
LLM_INPUT_PRICE_PER_MILLION = 0.05
LLM_OUTPUT_PRICE_PER_MILLION = 0.40
LLM_PRICING_VERSION = "2026-09-21"
