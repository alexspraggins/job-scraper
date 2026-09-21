# Job Scraper

A local job-search pipeline that runs focused searches across Indeed and LinkedIn,
filters titles for early-career software roles, stores canonical records in SQLite,
and generates clean CSV exports.

## Setup

```bash
uv sync --dev
uv run pytest -q
```

The project uses Python 3.14 and `uv`. Dependencies and the project itself are
installed into `.venv` from `pyproject.toml` and `uv.lock`.

## CLI reference

The general command shape is:

```bash
uv run job-scraper [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]
```

Running `job-scraper` without a command starts continuous search mode. Global
options must appear before the command name.

### Help

Show the top-level command list:

```bash
uv run job-scraper --help
```

Show help for an individual command:

```bash
uv run job-scraper run --help
uv run job-scraper list --help
uv run job-scraper status --help
uv run job-scraper export --help
```

### Global options

| Option | Description |
|---|---|
| `--database PATH` | Use a different SQLite database instead of `data/jobs.sqlite3`. |
| `--output-dir PATH` | Write CSV exports somewhere other than `data/exports`. |
| `-h`, `--help` | Show help and exit. |

Example with custom storage locations:

```bash
uv run job-scraper \
  --database private/jobs.sqlite3 \
  --output-dir private/exports \
  run --once
```

### `job-scraper` or `job-scraper run`

Run searches continuously. The first cycle searches the previous 24 hours;
later cycles run hourly with an overlapping two-hour lookback. Stop safely with
`Ctrl+C`.

```bash
uv run job-scraper
uv run job-scraper run
```

Available `run` options:

| Option | Description |
|---|---|
| `--once` | Run one search cycle and exit instead of waiting for the next cycle. |
| `--lookback-hours HOURS` | Override the first cycle's default 24-hour search window. |

Examples:

```bash
# Normal one-time search
uv run job-scraper run --once

# Search postings from the previous 48 hours
uv run job-scraper run --once --lookback-hours 48
```

### `job-scraper list`

Display stored jobs, newest first.

```bash
uv run job-scraper list
```

Available `list` options:

| Option | Description |
|---|---|
| `--status STATUS` | Show only `new`, `reviewed`, `saved`, `applied`, or `rejected` jobs. |
| `--limit NUMBER` | Limit the output; the default is 50 jobs. |

Examples:

```bash
uv run job-scraper list --status new --limit 25
uv run job-scraper list --status applied
```

### `job-scraper status`

Change a stored job's workflow status using the numeric ID shown by `list`.

```bash
uv run job-scraper status JOB_ID STATUS
```

Supported statuses are `new`, `reviewed`, `saved`, `applied`, and `rejected`.
Use `--note TEXT` to save or replace the job's current note. Every status change
is also recorded in SQLite history.

Examples:

```bash
uv run job-scraper status 42 reviewed
uv run job-scraper status 42 saved --note "Strong backend fit"
uv run job-scraper status 42 applied --note "Applied on company site"
uv run job-scraper status 42 rejected --note "Requires 5 years experience"
```

### `job-scraper export`

Regenerate `current_jobs.csv` from the canonical SQLite database without
running a new search.

```bash
uv run job-scraper export
```

### `uv` development commands

| Command | Description |
|---|---|
| `uv lock` | Refresh `uv.lock` from `pyproject.toml` without forcing upgrades. |
| `uv lock --upgrade` | Resolve the newest compatible dependency versions. |
| `uv sync --dev` | Synchronize `.venv`, including test dependencies. |
| `uv run pytest -q` | Run the automated tests with concise output. |
| `uv run pytest -v` | Run tests and show each test name. |
| `uv run python -m compileall -q src` | Check that every source file compiles. |

Search groups, title rules, sources, limits, and timing are configured in
`src/job_scraper/config.py`. Indeed and LinkedIn are enabled by default.
Skillsire remains supported but disabled.

Each source/query attempt has a 45-second process timeout. A stalled source is
terminated and recorded as an error so the remaining searches can continue.

## Stored data

SQLite is the source of truth:

```text
data/jobs.sqlite3
```

The database tracks canonical jobs, source-specific postings, matched role
terms, scrape attempts, workflow statuses, and status history. A repeated
listing updates `last_seen_at` without overwriting personal status or notes.

CSV files are generated from SQLite:

```text
data/exports/current_jobs.csv
data/exports/runs/YYYYMMDD_HHMMSS_new_jobs.csv
```

`current_jobs.csv` is a consolidated view. A per-run file is created only when
the run discovers new canonical jobs.

## Matching behavior

Titles are normalized before matching, including punctuation, spacing, and
hyphen variants. Matching uses phrase boundaries so short terms such as `C`,
`C++`, and `SRE` do not accidentally match parts of unrelated words.

Listings with seniority terms such as `senior`, `staff`, `principal`, `lead`,
`manager`, `director`, or `architect` are excluded. Accepted jobs receive a
role family, matched terms, and an `entry` or `unspecified` seniority label.

Each run prints raw, accepted, excluded, unmatched, duplicate, new, and error
counts. A failure from one source or query does not discard successful results
from the other attempts.

## Email notifications

Email is disabled by default. Set these environment variables to enable it:

```bash
export JOB_SCRAPER_EMAIL_ENABLED=true
export JOB_SCRAPER_EMAIL_FROM="sender@example.com"
export JOB_SCRAPER_EMAIL_PASSWORD="app-password"
export JOB_SCRAPER_EMAIL_TO="recipient@example.com"
export JOB_SCRAPER_EMAIL_SMTP="smtp.example.com"
```

Email is sent only when a run discovers new jobs, and the attachment contains
only that run's new listings. Do not commit credentials to the repository.

## Development

```bash
uv run pytest -v
uv run python -m compileall -q src
```

Tests use temporary databases and mocked scraper responses; they do not contact
live job boards.
