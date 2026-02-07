"""Vremenar common utils."""

from __future__ import annotations

from enum import StrEnum


class CountryID(StrEnum):
    """Supported countries ID enum."""

    Slovenia = "si"
    Germany = "de"

    def label(self) -> str:
        """Get country label."""
        if self is CountryID.Slovenia:
            return "Slovenia"
        if self is CountryID.Germany:
            return "Germany"
        raise RuntimeError  # pragma: no cover

    def full_name(self) -> str:
        """Get country full name."""
        if self is CountryID.Slovenia:
            return "slovenia"
        if self is CountryID.Germany:
            return "germany"
        raise RuntimeError  # pragma: no cover


class LanguageID(StrEnum):
    """Supported languages ID enum."""

    English = "en"
    German = "de"
    Slovenian = "sl"


class DatabaseType(StrEnum):
    """Database type enum."""

    Staging = "staging"
    Production = "production"
    Test = "test"
