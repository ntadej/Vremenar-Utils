"""Vremenar common utils."""
from enum import Enum


class CountryID(str, Enum):
    """Supported countries ID enum."""

    Slovenia = 'si'
    Germany = 'de'

    def label(self) -> str:
        """Get country label."""
        if self is CountryID.Slovenia:
            return 'Slovenia'
        if self is CountryID.Germany:
            return 'Germany'

    def full_name(self) -> str:
        """Get country full name."""
        if self is CountryID.Slovenia:
            return 'slovenia'
        if self is CountryID.Germany:
            return 'germany'


class LanguageID(str, Enum):
    """Supported languages ID enum."""

    English = 'en'
    German = 'de'
    Slovenian = 'sl'
