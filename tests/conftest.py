"""Tests configuration."""
import pytest

from os import environ


@pytest.fixture(autouse=True)
def env() -> dict[str, str]:
    """Return environment for tests."""
    values: dict[str, str] = {'VREMENAR_DATABASE': 'test'}
    for key, value in values.items():
        environ[key] = value
    return values
