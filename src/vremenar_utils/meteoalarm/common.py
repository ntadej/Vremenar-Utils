"""MeteoAlarm common utils."""
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ..cli.common import LanguageID


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

    Shelter = 'shelter'  # Take shelter in place or per instructions
    Evacuate = 'evacuate'  # Relocate as instructed in instructions
    Prepare = 'prepare'  # Make preparations per instructions
    Execute = 'execute'  # Execute a pre-planned activity identified in instructions
    Avoid = 'avoid'  # Avoid the subject event as per instructions
    Monitor = 'monitor'  # Attend to information sources as described in instructions
    AllClear = 'allclear'  # The subject event no longer poses a threat or concern and
    # any follow on action is described in instructions
    NoResponse = 'none'  # No recommended action


class AlertUrgency(Enum):
    """Alert urgency."""

    Immediate = 'immediate'  # Responsive action should be taken immediately.
    Expected = 'expected'  # Responsive action should be taken within the next hour.
    Future = 'future'  # Responsive action should be taken in the near future.
    Past = 'past'  # Responsive action no longer required.


class AlertSeverity(Enum):
    """Alert severity."""

    Minor = 'minor'  # yellow
    Moderate = 'moderate'  # orange
    Severe = 'severe'  # red
    Extreme = 'extreme'  # violet


class AlertCertainty(Enum):
    """Alert certainty."""

    Observed = 'observed'
    Likely = 'likely'  # p > 50 %
    Possible = 'possible'  # p < 50 %
    Unlikely = 'unlikely'  # p < 5 %


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

    def to_dict(self) -> dict[str, Any]:
        """Get dictionary with class properties."""
        return {'code': self.code, 'name': self.name, 'polygons': self.polygons}

    @classmethod
    def from_dict(cls, dictionary: dict[str, Any]) -> 'AlertArea':
        """Read AlertArea from a dictionary."""
        return cls(dictionary['code'], dictionary['name'], dictionary['polygons'])


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

        self.event: dict[LanguageID, str] = {}
        self.headline: dict[LanguageID, str] = {}
        self.description: dict[LanguageID, str] = {}
        self.instructions: dict[LanguageID, str] = {}
        self.sender_name: dict[LanguageID, str] = {}
        self.web: dict[LanguageID, str] = {}

    def __repr__(self) -> str:
        """Get string representation of an alert."""
        return (
            f'{self.id}: {self.event[LanguageID.English]}'
            f' ({self.type}, {self.severity}, {self.urgency}, {self.response_type})'
            f' ({self.onset} - {self.expires})'
        )

    def to_info_dict(self) -> dict[str, str]:
        """Get dictionary with common properties."""
        return {
            'id': self.id,
            'type': self.type.value,
            'urgency': self.urgency.value,
            'severity': self.severity.value,
            'certainty': self.certainty.value,
            'response_type': self.response_type.value,
            'onset': f'{str(self.onset.timestamp())}000',
            'expires': f'{str(self.expires.timestamp())}000',
        }

    def to_localised_dict(self, language: LanguageID) -> dict[str, str]:
        """Get dictionary with localised properties."""
        output: dict[str, str] = {}
        if self.event:
            output['event'] = (
                self.event[language]
                if language in self.event
                else self.event[LanguageID.English]
            )
        else:
            output['event'] = ''

        if self.headline:
            output['headline'] = (
                self.headline[language]
                if language in self.headline
                else self.headline[LanguageID.English]
            )
        else:
            output['headline'] = ''

        if self.description:
            output['description'] = (
                self.description[language]
                if language in self.description
                else self.description[LanguageID.English]
            )
        else:
            output['description'] = ''

        if self.instructions:
            output['instructions'] = (
                self.instructions[language]
                if language in self.instructions
                else self.instructions[LanguageID.English]
            )
        else:
            output['instructions'] = ''

        if self.sender_name:
            output['sender_name'] = (
                self.sender_name[language]
                if language in self.sender_name
                else self.sender_name[LanguageID.English]
            )
        else:
            output['sender_name'] = ''

        if self.web:
            output['web'] = (
                self.web[language]
                if language in self.web
                else self.web[LanguageID.English]
            )
        else:
            output['web'] = ''

        return output

    @classmethod
    def from_dict(cls, dictionary: dict[str, Any]) -> 'AlertInfo':
        """Read AlertInfo from a dictionary."""
        alert = cls(dictionary['key'])
        alert.areas = set(dictionary['areas'])
        alert.type = AlertType(dictionary['type'])
        alert.urgency = AlertUrgency(dictionary['urgency'])
        alert.severity = AlertSeverity(dictionary['severity'])
        alert.certainty = AlertCertainty(dictionary['certainty'])
        alert.response_type = AlertResponseType(dictionary['response_type'])
        alert.onset = datetime.fromtimestamp(dictionary['onset'], timezone.utc)
        alert.expires = datetime.fromtimestamp(dictionary['expires'], timezone.utc)
        alert.event = {LanguageID(k): v for k, v in dictionary['event'].items()}
        alert.headline = {LanguageID(k): v for k, v in dictionary['headline'].items()}
        alert.description = {
            LanguageID(k): v for k, v in dictionary['description'].items()
        }
        alert.instructions = {
            LanguageID(k): v for k, v in dictionary['instructions'].items()
        }
        alert.sender_name = {
            LanguageID(k): v for k, v in dictionary['sender_name'].items()
        }
        alert.web = {LanguageID(k): v for k, v in dictionary['web'].items()}
        return alert


class AlertNotificationInfo:
    """Alert notification info."""

    def __init__(self, id: str) -> None:
        """Initialise alert notification info."""
        self.id: str = id
        self.announce: int = 0
        self.onset: int = 0

    def __repr__(self) -> str:
        """Represent alert notification info as string."""
        return f'{self.id}: {self.announce}/{self.onset}'

    def to_dict(self) -> dict[str, Any]:
        """Get dictionary with class properties."""
        return {'announce': self.announce, 'onset': self.onset}

    @classmethod
    def from_dict(cls, dictionary: dict[str, Any]) -> 'AlertNotificationInfo':
        """Read alert notification info from a dictionary."""
        info = cls(dictionary['key'])
        info.announce = dictionary['announce']
        info.onset = dictionary['onset']
        return info
