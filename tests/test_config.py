"""Tests for environment parsing and configuration precedence."""

import importlib
from pathlib import Path

import pytest
import dotenv

from job_scraper import config


def reload_config(monkeypatch, *, dotenv_path=None, **values):
    for name in (
        "JOB_SCRAPER_INDEED_MAX_WORKERS",
        "JOB_SCRAPER_LINKEDIN_DESCRIPTION_LIMIT",
        "JOB_SCRAPER_LLM_ENABLED",
        "JOB_SCRAPER_LLM_MODEL",
        "JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD",
        "JOB_SCRAPER_LLM_MAX_CALLS_PER_CYCLE",
        "JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS",
        "JOB_SCRAPER_ENRICHMENT_MAX_SECONDS",
        "JOB_SCRAPER_ENRICHMENT_BATCH_SIZE",
        "JOB_SCRAPER_ENRICHMENT_CURRENT_RUN_SHARE",
        "JOB_SCRAPER_OPENAI_REQUEST_TIMEOUT_SECONDS",
        "JOB_SCRAPER_VERBOSE_LOGGING",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    if dotenv_path is None:
        monkeypatch.setattr(dotenv, "load_dotenv", lambda **_: False)
    else:
        real_load_dotenv = dotenv.load_dotenv
        monkeypatch.setattr(
            dotenv,
            "load_dotenv",
            lambda **kwargs: real_load_dotenv(dotenv_path, **kwargs),
        )
    return importlib.reload(config)


def test_configuration_uses_code_defaults_when_environment_is_empty(monkeypatch):
    loaded = reload_config(monkeypatch)

    assert loaded.INDEED_MAX_WORKERS == 3
    assert loaded.LINKEDIN_DESCRIPTION_LIMIT == 50
    assert loaded.LLM_ENABLED is False
    assert loaded.LLM_MODEL == "gpt-5-nano"
    assert loaded.LLM_MONTHLY_BUDGET_USD == 3.00
    assert loaded.LLM_MAX_CALLS_PER_CYCLE == 50
    assert loaded.LLM_MAX_OUTPUT_TOKENS == 6000
    assert loaded.ENRICHMENT_MAX_SECONDS == 300
    assert loaded.ENRICHMENT_BATCH_SIZE == 10
    assert loaded.ENRICHMENT_CURRENT_RUN_SHARE == 70
    assert loaded.OPENAI_REQUEST_TIMEOUT_SECONDS == 60.0
    assert loaded.VERBOSE_LOGGING is False


def test_environment_values_override_code_defaults(monkeypatch):
    loaded = reload_config(
        monkeypatch,
        JOB_SCRAPER_INDEED_MAX_WORKERS="4",
        JOB_SCRAPER_LINKEDIN_DESCRIPTION_LIMIT="12",
        JOB_SCRAPER_LLM_ENABLED="yes",
        JOB_SCRAPER_LLM_MODEL="custom-model",
        JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD="4.25",
        JOB_SCRAPER_LLM_MAX_CALLS_PER_CYCLE="6",
        JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS="7000",
        JOB_SCRAPER_OPENAI_REQUEST_TIMEOUT_SECONDS="12.5",
        JOB_SCRAPER_VERBOSE_LOGGING="on",
    )

    assert loaded.INDEED_MAX_WORKERS == 4
    assert loaded.LINKEDIN_DESCRIPTION_LIMIT == 12
    assert loaded.LLM_ENABLED is True
    assert loaded.LLM_MODEL == "custom-model"
    assert loaded.LLM_MONTHLY_BUDGET_USD == 4.25
    assert loaded.LLM_MAX_CALLS_PER_CYCLE == 6
    assert loaded.LLM_MAX_OUTPUT_TOKENS == 7000
    assert loaded.OPENAI_REQUEST_TIMEOUT_SECONDS == 12.5
    assert loaded.VERBOSE_LOGGING is True


def test_dotenv_values_override_code_defaults(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    Path(".env").write_text(
        "JOB_SCRAPER_LLM_MODEL=dotenv-model\n"
        "JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS=6500\n",
        encoding="utf-8",
    )
    loaded = reload_config(monkeypatch, dotenv_path=Path(".env"))

    assert loaded.LLM_MODEL == "dotenv-model"
    assert loaded.LLM_MAX_OUTPUT_TOKENS == 6500


def test_explicit_environment_values_override_dotenv(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    Path(".env").write_text(
        "JOB_SCRAPER_LLM_MODEL=dotenv-model\n"
        "JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS=6500\n",
        encoding="utf-8",
    )
    loaded = reload_config(
        monkeypatch,
        dotenv_path=Path(".env"),
        JOB_SCRAPER_LLM_MODEL="shell-model",
    )

    assert loaded.LLM_MODEL == "shell-model"
    assert loaded.LLM_MAX_OUTPUT_TOKENS == 6500


def test_blank_values_use_defaults_for_typed_settings(monkeypatch):
    loaded = reload_config(
        monkeypatch,
        JOB_SCRAPER_LLM_ENABLED="",
        JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS="",
        JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD="",
    )

    assert loaded.LLM_ENABLED is False
    assert loaded.LLM_MAX_OUTPUT_TOKENS == 6000
    assert loaded.LLM_MONTHLY_BUDGET_USD == 3.00


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        (
            "JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS",
            "many",
            "Invalid integer for JOB_SCRAPER_LLM_MAX_OUTPUT_TOKENS",
        ),
        (
            "JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD",
            "unlimited",
            "Invalid number for JOB_SCRAPER_LLM_MONTHLY_BUDGET_USD",
        ),
    ],
)
def test_invalid_numeric_values_identify_the_setting(monkeypatch, name, value, message):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=message):
        importlib.reload(config)


def test_config_module_exports_existing_public_settings(monkeypatch):
    loaded = reload_config(monkeypatch)

    assert hasattr(loaded, "LLM_ENABLED")
    assert hasattr(loaded, "LLM_MODEL")
    assert hasattr(loaded, "INDEED_MAX_WORKERS")
