# Job Scraper

Job Scraper searches Indeed and LinkedIn for early-career software roles,
filters and deduplicates the results, stores them in SQLite, optionally
extracts requirements with OpenAI, and writes readable CSV exports.

The normal pipeline is:

```text
source search → title filtering → canonical SQLite job → enrichment queue → CSV export
```

SQLite is the source of truth. CSV files are generated views and can be
regenerated at any time with `export`.

Indeed listings normally include a description in the JobSpy result and go
directly to requirement analysis. LinkedIn listings are first queued for a
separate public-description fetch, then queued for analysis.

## Quick start

### Requirements

- Python 3.14 or newer
- [`uv`](https://docs.astral.sh/uv/)
- Network access for live Indeed/LinkedIn searches
- An OpenAI API key only if LLM enrichment is enabled

### Install and test

```bash
uv sync --dev
uv run pytest -q
```

The automated tests use temporary SQLite databases and mocked scrapers. They
do not contact job boards or OpenAI.

### Run the first scrape

```bash
uv run job-scraper run --once
```

The first cycle searches the previous 24 hours. It writes:

```text
data/jobs.sqlite3
data/exports/current-jobs.csv
data/exports/runs/new-jobs-YYYY-MM-DD-HH-MM-SS.csv
```

The per-run CSV is created only when new canonical jobs are found. To inspect
the stored jobs:

```bash
uv run job-scraper list --status new --limit 25
uv run job-scraper show JOB_ID
```

Replace `JOB_ID` with the numeric ID printed by `list`.

### Enable optional OpenAI enrichment

LLM analysis is disabled by default. Install the optional dependency and create
a local environment file:

```bash
uv sync --dev --extra llm
cp .env.example .env
```

Edit `.env`:

```env
OPENAI_API_KEY=your-api-key
JOB_SCRAPER_LLM_ENABLED=true
```

The default monthly API budget is `$3.00`. OpenAI API billing is separate from
any ChatGPT subscription.

## Daily workflow

### Run one cycle

Use a one-time cycle when you want to review results immediately:

```bash
uv run job-scraper run --once
```

Use continuous mode when the process should search repeatedly:

```bash
uv run job-scraper run
```

The first continuous cycle uses the 24-hour lookback. Later cycles run hourly
with a two-hour overlapping lookback. Stop continuous mode with `Ctrl+C`.

For a different one-time lookback:

```bash
uv run job-scraper run --once --lookback-hours 48
```

### Review and update jobs

```bash
uv run job-scraper list --status new --limit 25
uv run job-scraper show JOB_ID
uv run job-scraper status JOB_ID reviewed
uv run job-scraper status JOB_ID saved --note "Strong backend fit"
uv run job-scraper status JOB_ID applied --note "Applied on company site"
uv run job-scraper status JOB_ID rejected --note "Requires too much experience"
```

Supported statuses are `new`, `reviewed`, `saved`, `applied`, and `rejected`.
Status changes preserve the job record and are recorded in SQLite history.
Rejecting a job cancels unfinished enrichment tasks for that job.

### Regenerate exports

```bash
uv run job-scraper export
```

This does not run a new search. It regenerates `current-jobs.csv` from SQLite
after reapplying the current title eligibility rules.

### Diagnose timing and job/task activity

Enable detailed logs for one run:

```bash
uv run job-scraper run --once --verbose
uv run job-scraper enrich --verbose
```

Or enable them in `.env`:

```env
JOB_SCRAPER_VERBOSE_LOGGING=true
```

Scrape logs include the source, search term, result count, duration, and stored
`job_id`. Enrichment logs include `job_id`, `posting_id`, `task_id`, stage,
outcome, title, and duration.

Example:

```text
scrape: run=5 source=linkedin term='computer vision engineer' outcome=completed results=15 duration_ms=5969
scrape: run=5 job_id=213 source=linkedin action=stored new=False title='Computer Vision Engineer'
enrichment: job_id=253 posting_id=870 task_id=15333 stage=analysis outcome=completed duration_ms=17928 title='GCP Cloud Engineer'
```

## Enrichment workflows

### Automatic processing

Every scrape synchronizes newly stored or changed postings with the durable
SQLite enrichment queue. Queue work is processed after the source searches and
before the run exports are written.

The automatic worker reserves up to seven slots for tasks created by the
current run and up to three slots for backlog or retry tasks. The configured
automatic limits are:

- up to 10 LinkedIn description fetches per cycle
- up to 10 OpenAI analyses per cycle

Tasks survive process restarts. A 15-minute lease allows interrupted work to
be recovered. Transient failures retry after one hour, then six hours. The
third failed attempt moves a task to `dead`.

### Process queued work manually

```bash
uv run job-scraper enrich --limit 20
```

`--limit` defaults to 20 for this manual command. It applies to both fetch and
analysis workers.

Fetch LinkedIn descriptions without making OpenAI calls:

```bash
uv run job-scraper enrich --fetch-only --limit 1 --verbose
```

Completing a LinkedIn fetch creates an analysis task, so a later normal
`enrich` command can process the extracted description.

### Repair or backfill missing tasks

Normal scrape, export, queue inspection, retry, and show commands do not scan
every existing job for missing tasks. Newly stored jobs are synchronized
immediately.

After an interrupted run, database migration, or manual data repair, explicitly
backfill missing tasks:

```bash
uv run job-scraper enrich --migrate-existing --limit 20
```

This scans all eligible stored jobs before processing due work. Use it as a
repair operation rather than on every normal scrape.

### Retry dead tasks

Inspect dead tasks:

```bash
uv run job-scraper queue --status dead
```

Retry one task or all dead tasks for a job:

```bash
uv run job-scraper retry TASK_ID
uv run job-scraper retry --job-id JOB_ID
```

Only tasks in the `dead` state are reset by `retry`.

## Monitoring and troubleshooting

### Inspect queue state

```bash
uv run job-scraper queue
uv run job-scraper queue --status retry
uv run job-scraper queue --status dead --limit 50
```

Possible task states include `pending`, `leased`, `retry`, `completed`,
`dead`, `cancelled`, and `budget_blocked`.

### Inspect API usage and budget

```bash
uv run job-scraper usage
uv run job-scraper usage --month 2026-09
uv run job-scraper usage --outstanding
```

The usage report includes calls, input/output tokens, counted cost, and
remaining monthly budget. Outstanding reservations are calls whose process
ended before the result was fully recorded. Use `--resolve` only when you know
the actual cost:

```bash
uv run job-scraper usage --resolve RESERVATION_TOKEN --actual-cost 0.0042
```

### Troubleshooting table

| Symptom | Check | Next action |
|---|---|---|
| No jobs in the CSV | Run summary counts and `list` | Check whether results were excluded, unmatched, or duplicates; use a larger `--lookback-hours`. |
| Source timeout or rate limit | Verbose run logs and summary warnings | Wait for the source retry window; do not repeatedly hammer the source. |
| LinkedIn description missing | `queue --status retry` or `queue --status dead` | Retry the task after the source recovers; inspect the task error. |
| Analysis task is retrying | `queue --status retry` | Run `enrich --verbose`; check structured-output or evidence-validation errors. |
| Tasks are budget blocked | `usage` and `.env` budget setting | Wait for the next UTC month or raise the configured budget deliberately. |
| A job has no enrichment task | `show JOB_ID` and queue summary | Run `enrich --migrate-existing`. |
| CSV lacks expected skills | `show JOB_ID` and `analysis_status` | Process pending work with `enrich`, then run `export`. |
| OpenAI configuration error | `OPENAI_API_KEY`, optional dependency, and `JOB_SCRAPER_LLM_ENABLED` | Run `uv sync --dev --extra llm`, configure the key, and retry. |

## Complete command reference

General form:

```bash
uv run job-scraper [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]
```

Global options must appear before the command:

```bash
uv run job-scraper --database private/jobs.sqlite3 run --once
```

| Global option | Default | Description |
|---|---|---|
| `--database PATH` | `data/jobs.sqlite3` | SQLite database path. |
| `--output-dir PATH` | `data/exports` | CSV export directory. |
| `-h`, `--help` | — | Show help. |

### `run`

```bash
uv run job-scraper run [--once] [--lookback-hours HOURS] [--verbose]
```

| Option | Description |
|---|---|
| `--once` | Run one cycle and exit. Without it, continue hourly. |
| `--lookback-hours HOURS` | Override the first cycle's lookback; default is 24 hours. |
| `--verbose` | Log scrape and enrichment timing details. |

### `list`

```bash
uv run job-scraper list [--status STATUS] [--limit NUMBER]
```

`--status` accepts `new`, `reviewed`, `saved`, `applied`, or `rejected`.
`--limit` defaults to 50.

### `status`

```bash
uv run job-scraper status JOB_ID STATUS [--note TEXT]
```

Requires one numeric `JOB_ID` and one supported status.

### `export`

```bash
uv run job-scraper export
```

Regenerates the consolidated CSV without scraping.

### `queue`

```bash
uv run job-scraper queue [--status STATUS] [--limit NUMBER]
```

Without `--status`, prints counts grouped by task type and status. With a
status, prints matching task IDs, job IDs, attempt counts, and latest errors.
`--limit` defaults to 50.

### `retry`

```bash
uv run job-scraper retry [TASK_ID] [--job-id JOB_ID]
```

Provide exactly one task ID or job ID. Resets matching dead tasks to pending.

### `enrich`

```bash
uv run job-scraper enrich [--limit NUMBER] [--fetch-only]
                         [--migrate-existing] [--verbose]
```

| Option | Default | Description |
|---|---:|---|
| `--limit NUMBER` | `20` | Maximum fetches and analyses to claim. |
| `--fetch-only` | off | Fetch descriptions without running analysis. |
| `--migrate-existing` | off | Backfill missing tasks for all eligible jobs first. |
| `--verbose` | off | Log enrichment IDs, stages, outcomes, and durations. |

### `show`

```bash
uv run job-scraper show JOB_ID
```

Displays the job, source postings, description metadata, and current extracted
requirements with evidence.

### `usage`

```bash
uv run job-scraper usage [--month YYYY-MM] [--outstanding]
uv run job-scraper usage --resolve TOKEN --actual-cost USD
```

`--month` selects a UTC month. `--outstanding` lists reserved or uncertain
calls. `--resolve` requires `--actual-cost` and updates one outstanding usage
reservation.

## Configuration

`.env` is optional. The fallback defaults are defined once in
`src/job_scraper/config.py`; `.env.example` is only a template showing common
overrides. If you need local settings, copy it to `.env`—that file is ignored
by Git. Configuration precedence is:

1. Explicitly exported shell variables or process-manager settings
2. Values loaded from `.env`
3. Defaults in `src/job_scraper/config.py`

Blank secret values are safe when the related feature is disabled. For example,
you do not need `OPENAI_API_KEY` unless LLM enrichment is enabled, and you do
not need email credentials unless email notifications are enabled.

### Scraping

| Variable | Default | Description |
|---|---:|---|
| `JOB_SCRAPER_INDEED_MAX_WORKERS` | `3` | Concurrent Indeed search workers. LinkedIn remains serial. |
| `JOB_SCRAPER_LINKEDIN_DESCRIPTION_LIMIT` | `10` | Maximum LinkedIn fetches per automatic cycle. |

### OpenAI enrichment

| Variable | Default | Cost impact |
|---|---:|---|
| `OPENAI_API_KEY` | unset | Required for OpenAI calls. |
| `JOB_SCRAPER_LLM_ENABLED` | `false` | Enables API calls when true. |
| `JOB_SCRAPER_LLM_MODEL` | `gpt-5-nano` | Selects the model and analysis cache identity. |
| `JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD` | `3.00` | Hard monthly budget. |
| `JOB_SCRAPER_LLM_MAX_CALLS_PER_CYCLE` | `10` | Maximum analysis calls per automatic cycle. |
| `JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS` | `6000` | Maximum structured output per analysis; affects projected reservations. |

### Logging

| Variable | Default | Description |
|---|---:|---|
| `JOB_SCRAPER_VERBOSE_LOGGING` | `false` | Enable scrape and enrichment timing logs. |

The equivalent one-run CLI flags are `run --verbose` and `enrich --verbose`.

### Email

Email is disabled by default and is sent only when a run finds new jobs. Set all
of these values to enable it:

```env
JOB_SCRAPER_EMAIL_ENABLED=true
JOB_SCRAPER_EMAIL_FROM=sender@example.com
JOB_SCRAPER_EMAIL_PASSWORD=app-password
JOB_SCRAPER_EMAIL_TO=recipient@example.com
JOB_SCRAPER_EMAIL_SMTP=smtp.example.com
```

Never commit `.env`, API keys, SMTP passwords, or other credentials.

## Filtering and matching

The default configuration runs 15 representative searches across nine role
families. Broader title variants remain in the local matching rules, so a title
does not need to exactly equal the search phrase.

Titles are normalized for punctuation, spacing, and hyphen variants. Phrase
boundaries prevent short terms such as `C`, `C++`, and `SRE` from matching
inside unrelated words.

Unnumbered titles such as `Software Engineer` are accepted when they match a
configured role. Titles with levels II-X or 2-10, `mid-level`, `intermediate`,
`experienced`, and seniority terms such as `senior`, `staff`, `principal`,
`lead`, `manager`, `director`, or `architect` are excluded.

Existing records are reclassified before export. Ineligible records remain in
SQLite history but are omitted from the current CSV view.

Each run reports raw, accepted, excluded, unmatched, duplicate, new, and error
counts. A failure from one source or query does not discard successful results
from other attempts.

## Data and outputs

### SQLite

SQLite is stored at:

```text
data/jobs.sqlite3
```

Conceptually, the database contains:

- canonical jobs and user workflow statuses
- source-specific postings and descriptions
- scrape runs and per-source attempts
- enrichment tasks and task event history
- versioned posting analyses and extracted requirements
- OpenAI usage reservations and actual cost records

Identifiers have different scopes:

- `job_id`: canonical job shown by `list`, `show`, and CSV exports
- `posting_id`: one source-specific posting for a job
- `task_id`: one fetch or analysis queue task shown by `queue` and verbose logs

### CSV exports

```text
data/exports/current-jobs.csv
data/exports/runs/new-jobs-YYYY-MM-DD-HH-MM-SS.csv
```

`current-jobs.csv` is the consolidated eligible-job view. Per-run CSV files
contain only new canonical jobs from that run. Exported enrichment columns
include `required_skills`, `preferred_skills`, `experience`, and
`analysis_status`. Long descriptions and evidence remain in SQLite so the CSV
stays compact.

Generated databases, CSVs, caches, and local environments are ignored by Git.

## Testing and development

### Fast test suite

Run all mocked tests:

```bash
uv run pytest -q
```

If the execution environment cannot write pytest's local cache, use a writable
temporary cache directory:

```bash
./.venv/bin/python -m pytest -q \
  -o cache_dir=/private/tmp/job-scraper-pytest-cache
```

Focused suites:

```bash
./.venv/bin/python -m pytest tests/test_enrichment.py -q
./.venv/bin/python -m pytest tests/test_output.py tests/test_main.py -q
./.venv/bin/python -m pytest tests/test_storage.py tests/test_enrichment.py -q
```

The suite uses temporary databases and mocked network/LLM calls, so it is the
preferred feedback loop for code changes.

### Live smoke tests

Live commands contact external services and may be slow, rate-limited, or incur
OpenAI API cost. Use them after meaningful integration changes rather than
after every edit:

```bash
# LinkedIn fetch only; no OpenAI analysis
uv run job-scraper enrich --fetch-only --limit 1 --verbose

# One real OpenAI analysis
uv run job-scraper enrich --limit 1 --verbose

# One complete scrape, enrichment, and export cycle
uv run job-scraper run --once --verbose
```

### Other development commands

```bash
uv lock
uv sync --dev
uv sync --dev --extra llm
./.venv/bin/python -m compileall -q src
uv build
```

Tests should remain the primary development check; live smoke tests validate
external integrations and current source behavior.
