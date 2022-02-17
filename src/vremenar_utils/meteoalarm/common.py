"""MeteoAlarm comon utils."""
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO, TextIOWrapper
from json import load, JSONEncoder
from pkgutil import get_data
from typing import Any, Union


class AlertCountry(Enum):
    """Countries that have weather alerts handled by Vremenar."""

    Germany = 'germany'
    Slovenia = 'slovenia'

    def country_code(self) -> str:
        """Return country code."""
        if self is AlertCountry.Germany:
            return 'DE'
        if self is AlertCountry.Slovenia:
            return 'SI'


class AlertLanguage(Enum):
    """Supported languages for alerts."""

    English = 'en'
    German = 'de'
    Slovenian = 'sl'


class AlertType(Enum):
    """Alert type."""

    Generic = 'alert'
    Wind = 'wind'
    SnowIce = 'snow-ice'
    Thunderstorm = 'thunderstorm'
    Fog = 'fog'
    HighTemperature = 'high-temperature'
    LowTemperature = 'low-temperature'
    CoastalEvent = 'coastalevent'
    ForestFire = 'forest-fire'
    Avalanches = 'avalanches'
    Rain = 'rain'
    Flooding = 'flooding'
    RainFlood = 'rain-flood'


class AlertResponseType(Enum):
    """Alert response type."""

    Shelter = 'Shelter'  # Take shelter in place or per instructions
    Evacuate = 'Evacuate'  # Relocate as instructed in instructions
    Prepare = 'Prepare'  # Make preparations per instructions
    Execute = 'Execute'  # Execute a pre-planned activity identified in instructions
    Avoid = 'Avoid'  # Avoid the subject event as per instructions
    Monitor = 'Monitor'  # Attend to information sources as described in instructions
    AllClear = 'AllClear'  # The subject event no longer poses a threat or concern and
    # any follow on action is described in instructions
    NoResponse = 'None'  # No recommended action


class AlertUrgency(Enum):
    """Alert urgency."""

    Immediate = 'Immediate'  # Responsive action should be taken immediately.
    Expected = 'Expected'  # Responsive action should be taken within the next hour.
    Future = 'Future'  # Responsive action should be taken in the near future.
    Past = 'Past'  # Responsive action no longer required.


class AlertSeverity(Enum):
    """Alert severity."""

    Minor = 'Minor'  # green
    Moderate = 'Moderate'  # yellow
    Severe = 'Severe'  # orange
    Extreme = 'Extreme'  # red


class AlertCertainty(Enum):
    """Alert certainty."""

    Observed = 'Observed'
    Likely = 'Likely'  # p > 50 %
    Possible = 'Possible'  # p < 50 %
    Unlikely = 'Unlikely'  # p < 5 %


class AlertArea:
    """MeteoAlarm area."""

    def __init__(self, code: str, name: str, polygons: list[list[list[float]]]) -> None:
        """Initialise MeteoAlarm area."""
        self.code = code
        self.name = name
        self.polygons = polygons

    def __repr__(self) -> str:
        """Represent MeteoAlarm area as string."""
        return f'{self.code}: {self.name} ({len(self.polygons)} polygon(s))'


class AlertInfo:
    """MeteoAlarm alert info."""

    def __init__(self, id: str) -> None:
        """Initialise MeteoAlarm alert."""
        self.id: str = id
        self.areas: set[str] = set()

        self.type: AlertType = AlertType.Generic
        self.urgency: AlertUrgency = AlertUrgency.Expected
        self.severity: AlertSeverity = AlertSeverity.Minor
        self.certainty: AlertCertainty = AlertCertainty.Unlikely
        self.response_type: AlertResponseType = AlertResponseType.NoResponse

        self.onset: datetime
        self.expires: datetime

        self.event: dict[AlertLanguage, str] = {}
        self.headline: dict[AlertLanguage, str] = {}
        self.description: dict[AlertLanguage, str] = {}
        self.instructions: dict[AlertLanguage, str] = {}
        self.sender_name: dict[AlertLanguage, str] = {}
        self.web: dict[AlertLanguage, str] = {}

    def __repr__(self) -> str:
        """Get string representation of an alert."""
        return (
            f'{self.id}: {self.event[AlertLanguage.English]}'
            f' ({self.type}, {self.severity}, {self.urgency}, {self.response_type})'
            f' ({self.onset} - {self.expires})'
        )

    def to_dict(self) -> dict[str, Any]:
        """Get dictionary with class properties."""
        return {
            'id': self.id,
            'areas': list(self.areas),
            'type': self.type.value,
            'severity': self.severity.value,
            'certainty': self.certainty.value,
            'response_type': self.response_type.value,
            'onset': self.onset.timestamp(),
            'expires': self.expires.timestamp(),
            'event': {k.value: v for k, v in self.event.items()},
            'headline': {k.value: v for k, v in self.headline.items()},
            'description': {k.value: v for k, v in self.description.items()},
            'instructions': {k.value: v for k, v in self.instructions.items()},
            'sender_name': {k.value: v for k, v in self.sender_name.items()},
            'web': {k.value: v for k, v in self.web.items()},
        }

    @classmethod
    def from_dict(cls, dictionary: dict[str, Any]) -> 'AlertInfo':
        """Read AlertInfo from a dictionary."""
        alert = cls(dictionary['id'])
        alert.areas = set(dictionary['areas'])
        alert.type = AlertType(dictionary['type'])
        alert.severity = AlertSeverity(dictionary['severity'])
        alert.certainty = AlertCertainty(dictionary['certainty'])
        alert.response_type = AlertResponseType(dictionary['response_type'])
        alert.onset = datetime.fromtimestamp(dictionary['onset'], timezone.utc)
        alert.expires = datetime.fromtimestamp(dictionary['expires'], timezone.utc)
        alert.event = {AlertLanguage(k): v for k, v in dictionary['event'].items()}
        alert.headline = {
            AlertLanguage(k): v for k, v in dictionary['headline'].items()
        }
        alert.description = {
            AlertLanguage(k): v for k, v in dictionary['description'].items()
        }
        alert.instructions = {
            AlertLanguage(k): v for k, v in dictionary['instructions'].items()
        }
        alert.sender_name = {
            AlertLanguage(k): v for k, v in dictionary['sender_name'].items()
        }
        alert.web = {AlertLanguage(k): v for k, v in dictionary['web'].items()}
        return alert


class AlertEncoder(JSONEncoder):
    """Common JSON encoder."""

    def default(self, o: Any) -> Any:
        """Return default value of JSON encoder."""
        if isinstance(o, AlertArea):
            return o.__dict__
        if isinstance(o, set):
            return list(o)


def load_stations(
    country: AlertCountry,
) -> dict[str, dict[str, Union[str, int, float]]]:
    """Load stations for a specific country."""
    if country is AlertCountry.Germany:
        from ..dwd.stations import load_stations as dwd_stations

        return dwd_stations()

    if country is AlertCountry.Slovenia:
        data = get_data('vremenar_utils', 'data/stations/ARSO.json')
        if data:
            bytes = BytesIO(data)
            with TextIOWrapper(bytes, encoding='utf-8') as file:
                stations = load(file)
        output: dict[str, dict[str, Union[str, int, float]]] = {}
        for station in stations:
            output[station['id']] = station
        return output
