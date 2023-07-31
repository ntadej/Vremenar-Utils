"""Tests configuration."""
from os import environ

import pytest


@pytest.fixture(autouse=True)
def env() -> dict[str, str]:
    """Return environment for tests."""
    values: dict[str, str] = {
        "VREMENAR_UTILS_CONFIG": "run/test.yml",
        "VREMENAR_DATABASE": "test",
    }
    for key, value in values.items():
        environ[key] = value
    return values
