"""Application configuration for the job scraper."""

import os
from pathlib import Path


SEARCH_GROUPS: dict[str, list[str]] = {
    "core_software": [
        "software engineer",
        "software developer",
        "software development engineer",
    ],
    "backend_full_stack": [
        "backend engineer",
        "backend developer",
        "full stack developer",
    ],
    "language_specific": [
        "python developer",
        "java developer",
        "c++ developer",
        "golang developer",
    ],
    "embedded_systems": [
        "embedded software engineer",
        "firmware engineer",
        "systems software engineer",
    ],
    "platform_cloud": [
        "platform engineer",
        "cloud engineer",
        "site reliability engineer",
    ],
    "data": [
        "data engineer",
        "etl developer",
    ],
    "quality_automation": [
        "test automation engineer",
        "sdet",
        "software test engineer",
    ],
    "robotics_edge": [
        "robotics software engineer",
        "computer vision engineer",
        "iot engineer",
    ],
    "integration_research": [
        "integration engineer",
        "research software engineer",
        "simulation software engineer",
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
    "entry level",
    "junior",
    "associate",
    "new grad",
    "graduate",
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
