"""Skillsire API integration."""

import json

import pandas as pd
import requests


def construct_skillsire_job_url(job_id: object) -> str:
    return f"https://www.skillsire.com/job/jobs-enlisting/all-jobs?jobId={job_id}"


def _fetch_jobs(initial_payload: dict, results_fetch_count: int) -> list[dict]:
    api_url = "https://www.skillsire.com/api/job/all-jobs"
    headers = {"Content-Type": "application/json"}
    all_jobs: list[dict] = []
    offset = 0
    batch_size = 20

    while len(all_jobs) < results_fetch_count:
        payload = {**initial_payload, "offset": offset}
        try:
            response = requests.post(
                api_url,
                data=json.dumps(payload),
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as error:
            print(f"Skillsire request failed: {error}")
            break

        jobs = data.get("jobs", [])
        all_jobs.extend(jobs)
        total_jobs = min(
            data.get("metaData", {}).get("resultCount", 0),
            results_fetch_count,
        )
        print(
            f"Fetched {len(jobs)} jobs (offset: {offset}), "
            f"total so far from Skillsire: {len(all_jobs)} of {total_jobs}."
        )

        if not jobs or len(all_jobs) >= total_jobs:
            break
        offset += batch_size

    return all_jobs[:results_fetch_count]


def scrape_skillsire(
    current_jobs: pd.DataFrame,
    hours: int,
    search_term: str,
    results_fetch_count: int,
) -> pd.DataFrame:
    """Fetch Skillsire jobs and append them in the common job schema."""
    skillsire_hours = "p24h" if hours > 1 else "p1h"
    payload = {
        "loc": "United States",
        "cc": "us",
        "type": "country",
        "dp": skillsire_hours,
        "query": search_term,
    }
    skillsire_jobs = _fetch_jobs(payload, results_fetch_count)

    normalized_jobs = []
    for job in skillsire_jobs:
        locations = job.get("jobLocations") or []
        location = locations[0].get("jobState", "Unknown") if locations else "Unknown"
        normalized_jobs.append(
            {
                "job_url": construct_skillsire_job_url(job.get("jobId")),
                "title": job.get("jobTitle", ""),
                "company": job.get("jobCompany", ""),
                "location": location,
            }
        )

    if not normalized_jobs:
        return current_jobs
    return pd.concat([current_jobs, pd.DataFrame(normalized_jobs)], ignore_index=True)
