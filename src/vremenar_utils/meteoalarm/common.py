"""MeteoAlarm comon utils."""
from enum import Enum
from io import BytesIO, TextIOWrapper
from json import load, JSONEncoder
from pkgutil import get_data

supported_languages = ['en-US', 'de-DE', 'sl-SI']


class AlarmCountry(Enum):
    """Countries that have weather alarms handled by Vremenar."""

    Germany = 'germany'
    Slovenia = 'slovenia'

    def country_code(self) -> str:
        """Return country code."""
        if self is AlarmCountry.Germany:
            return 'DE'
        if self is AlarmCountry.Slovenia:
            return 'SI'


class AlarmArea:
    """MeteoAlarm area."""

    def __init__(self, code: str, name: str, polygons: list[list[list[float]]]) -> None:
        """Initialise MeteoAlarm area."""
        self.code = code
        self.name = name
        self.polygons = polygons

    def __repr__(self) -> str:
        """Represent MeteoAlarm area as string."""
        return f'{self.code}: {self.name} ({len(self.polygons)} polygon(s))'


class AlarmEncoder(JSONEncoder):
    """Common JSON encoder."""

    def default(self, o):
        """Return default value of JSON encoder."""
        return o.__dict__


def load_stations(country: AlarmCountry) -> dict:
    """Load stations for a specific country."""
    if country is AlarmCountry.Germany:
        from ..dwd.stations import load_stations as dwd_stations

        return dwd_stations()

    if country is AlarmCountry.Slovenia:
        data = get_data('vremenar_utils', 'data/stations/ARSO.json')
        if data:
            bytes = BytesIO(data)
            with TextIOWrapper(bytes, encoding='utf-8') as file:
                stations = load(file)
        return stations
