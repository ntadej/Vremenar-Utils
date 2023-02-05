"""MeteoAlarm common utils."""
from datetime import datetime
from enum import Enum
from json import dumps

from vremenar_utils.cli.common import LanguageID


class AlertType(Enum):
    """Alert type."""

    Generic = "alert"
    Wind = "wind"
    SnowIce = "snow-ice"
    Thunderstorm = "thunderstorm"
    Fog = "fog"
    HighTemperature = "high-temperature"
    LowTemperature = "low-temperature"
    CoastalEvent = "coastalevent"
    ForestFire = "forest-fire"
    Avalanches = "avalanches"
    Rain = "rain"
    Flooding = "flooding"
    RainFlood = "rain-flood"


class AlertResponseType(Enum):
    """Alert response type."""

    Shelter = "shelter"  # Take shelter in place or per instructions
    Evacuate = "evacuate"  # Relocate as instructed in instructions
    Prepare = "prepare"  # Make preparations per instructions
    Execute = "execute"  # Execute a pre-planned activity identified in instructions
    Avoid = "avoid"  # Avoid the subject event as per instructions
    Monitor = "monitor"  # Attend to information sources as described in instructions
    AllClear = "allclear"  # The subject event no longer poses a threat or concern and
    # any follow on action is described in instructions
    NoResponse = "none"  # No recommended action


class AlertUrgency(Enum):
    """Alert urgency."""

    Immediate = "immediate"  # Responsive action should be taken immediately.
    Expected = "expected"  # Responsive action should be taken within the next hour.
    Future = "future"  # Responsive action should be taken in the near future.
    Past = "past"  # Responsive action no longer required.


class AlertSeverity(Enum):
    """Alert severity."""

    Minor = "minor"  # yellow
    Moderate = "moderate"  # orange
    Severe = "severe"  # red
    Extreme = "extreme"  # violet

    def topics(self) -> list[str]:
        """Get topics based on the severity."""
        if self is AlertSeverity.Minor:
            return ["minor"]
        if self is AlertSeverity.Moderate:
            return ["moderate", "minor"]
        if self is AlertSeverity.Severe:
            return ["severe", "moderate", "minor"]
        if self is AlertSeverity.Extreme:  # noqa: RET503
            return ["extreme", "severe", "moderate", "minor"]


class AlertCertainty(Enum):
    """Alert certainty."""

    Observed = "observed"
    Likely = "likely"  # p > 50 %
    Possible = "possible"  # p < 50 %
    Unlikely = "unlikely"  # p < 5 %


class AlertArea:
    """MeteoAlarm area."""

    def __init__(
        self,
        code: str,
        name: str,
        description: str,
        polygons: list[list[list[float]]],
    ) -> None:
        """Initialise MeteoAlarm area."""
        self.code = code
        self.name = name
        self.description = description
        self.polygons = polygons

    def __repr__(self) -> str:
        """Represent MeteoAlarm area as string."""
        return f"{self.code}: {self.name} ({len(self.polygons)} polygon(s))"

    def to_dict(self) -> dict[str, str | list[list[list[float]]]]:
        """Get dictionary with class properties."""
        return {
            "code": self.code,
            "name": self.name,
            "description": self.description,
            "polygons": self.polygons,
        }

    def to_dict_for_database(self) -> dict[str, str]:
        """Get dictionary with class properties for database usage."""
        return {
            "code": self.code,
            "name": self.name,
            "description": self.description,
            "polygons": dumps(self.polygons),
        }

    @classmethod
    def from_dict(
        cls,
        dictionary: dict[str, str | list[list[list[float]]]],
    ) -> "AlertArea":
        """Read AlertArea from a dictionary."""
        if (
            not isinstance(dictionary["code"], str)
            or not isinstance(dictionary["name"], str)
            or not isinstance(dictionary["description"], str)
        ):
            err = "'code' and 'name' need to be strings"
            raise ValueError(err)

        if not isinstance(dictionary["polygons"], list):
            err = "'polygons' needs to be a list of lists of floats"
            raise ValueError(err)

        return cls(
            dictionary["code"],
            dictionary["name"],
            dictionary["description"],
            dictionary["polygons"],
        )


class AlertInfo:
    """MeteoAlarm alert info."""

    def __init__(self, alert_id: str) -> None:
        """Initialise MeteoAlarm alert."""
        self.id: str = alert_id
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
            f"{self.id}: {self.event[LanguageID.English]}"
            f" ({self.type}, {self.severity}, {self.urgency}, {self.response_type})"
            f" ({self.onset} - {self.expires})"
        )

    def to_info_dict(self) -> dict[str, str]:
        """Get dictionary with common properties."""
        return {
            "id": self.id,
            "type": self.type.value,
            "urgency": self.urgency.value,
            "severity": self.severity.value,
            "certainty": self.certainty.value,
            "response_type": self.response_type.value,
            "onset": f"{str(int(self.onset.timestamp()))}000",
            "expires": f"{str(int(self.expires.timestamp()))}000",
        }

    def to_localised_dict(self, language: LanguageID) -> dict[str, str]:
        """Get dictionary with localised properties."""
        output: dict[str, str] = {}
        attributes = [
            "event",
            "headline",
            "description",
            "instructions",
            "sender_name",
            "web",
        ]

        for attribute in attributes:
            if hasattr(self, attribute) and getattr(self, attribute):
                value = getattr(self, attribute)
                output[attribute] = (
                    value[language] if language in value else value[LanguageID.English]
                )
            else:
                output[attribute] = ""

        return output


class AlertNotificationInfo:
    """Alert notification info."""

    def __init__(self, alert_id: str) -> None:
        """Initialise alert notification info."""
        self.id: str = alert_id
        self.announce: int = 0
        self.onset: int = 0

    def __repr__(self) -> str:
        """Represent alert notification info as string."""
        return f"{self.id}: {self.announce}/{self.onset}"

    def to_dict(self) -> dict[str, int]:
        """Get dictionary with class properties."""
        return {"announce": self.announce, "onset": self.onset}
