from job_scraper.skillsire import construct_skillsire_job_url


def test_construct_skillsire_url():
    assert construct_skillsire_job_url(123).endswith("jobId=123")
