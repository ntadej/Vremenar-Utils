"""MeteoAlarm parsing."""

# Inspired and based on https://github.com/rolfberkenbosch/meteoalert-api
from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from httpx import AsyncClient, codes
from xmltodict import parse  # type: ignore

from vremenar_utils.cli.common import CountryID, LanguageID

from . import TIMEOUT
from .common import (
    AlertCertainty,
    AlertInfo,
    AlertResponseType,
    AlertSeverity,
    AlertType,
    AlertUrgency,
)

if TYPE_CHECKING:
    from vremenar_utils.cli.logging import Logger

METEOALARM_ATOM_ENDPOINT = (
    "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-atom-{0}"
)


class MeteoAlarmParser:
    """MeteoAlarm Atom feed parser."""

    def __init__(
        self,
        logger: Logger,
        country: CountryID,
        existing_alerts: set[str],
    ) -> None:
        """Initialize MeteoAlarm parser."""
        self.logger: Logger = logger
        self.country: CountryID = country
        self.now: datetime = datetime.now(tz=UTC)
        self.existing_alert_ids: set[str] = existing_alerts
        self.obsolete_alert_ids: set[str] = set()

    async def get_new_alerts(self) -> set[tuple[str, str]]:
        """Retrieve new alerts."""
        endpoint = METEOALARM_ATOM_ENDPOINT.format(self.country.full_name())
        async with AsyncClient() as client:
            response = await client.get(endpoint, timeout=TIMEOUT)
        # can be invalid
        if response.status_code != codes.OK:  # pragma: no cover
            return set()

        result = response.text

        all_ids = set()
        new_ids = set()

        # Parse the XML response for the alert feed and loop over the entries
        feed_data = parse(result)
        feed = feed_data.get("feed", [])
        entries = feed.get("entry", [])
        for entry in entries if isinstance(entries, list) else [entries]:
            cap_id = entry.get("cap:identifier")
            all_ids.add(cap_id)
            if cap_id in self.existing_alert_ids:
                continue

            expires = self.parse_alert_datetime(entry.get("cap:expires"))
            if expires < self.now:
                continue

            # Get the cap URL for additional alert data
            cap_url = None
            for link in entry.get("link"):
                if link.get("@type") == "application/cap+xml":
                    cap_url = link.get("@href")

            if not cap_url:  # pragma: no cover
                continue

            new_ids.add((cap_id, cap_url))

        # check for alerts that are no longer present
        self.check_for_obsolete(all_ids)

        return new_ids

    def check_for_obsolete(self, all_ids: set[str]) -> None:
        """Check for obsolete alerts."""
        for alert_id in self.existing_alert_ids:
            if alert_id not in all_ids:  # pragma: no cover
                self.obsolete_alert_ids.add(alert_id)

    def parse_alert_datetime(self, string: str) -> datetime:
        """Parse alert date/time."""
        return datetime.strptime(string, "%Y-%m-%dT%H:%M:%S%z")

    async def parse_cap(
        self,
        alert_id: str,
        url: str,
        areas_desc_map: dict[str, str],
    ) -> AlertInfo | None:
        """Parse CAP."""
        alert = AlertInfo(alert_id)

        # Parse the XML response for the alert information
        async with AsyncClient() as client:
            response = await client.get(url, timeout=TIMEOUT)
        # can be missing
        if response.status_code != codes.OK:  # pragma: no cover
            return None

        result = response.text
        # can be empty
        if not result:  # pragma: no cover
            return None

        result = result[: result.rfind(">") + 1]  # ignore characters after last XML tag

        alert_data = parse(result)
        alert_dict = alert_data.get("alert", {})

        # get the alert data in the supported languages
        translations = alert_dict.get("info", [])

        if isinstance(translations, list):  # pragma: no branch
            for translation in translations:
                try:
                    lang = LanguageID(translation.get("language")[:2])
                except ValueError:  # pragma: no cover
                    continue
                self.parse_alert_info(alert, translation)
                self.parse_alert_translations(alert, lang, translation)
        else:  # pragma: no cover
            try:
                lang = LanguageID(translation.get("language")[:2])
            except ValueError:  # pragma: no cover
                lang = LanguageID.English
            translation = translations
            self.parse_alert_info(alert, translation)
            self.parse_alert_translations(alert, lang, translation)

        # parse parameters
        parameters = translation.get("parameter", [])
        try:
            self.parse_alert_parameters(alert, parameters)
        except ValueError:  # pragma: no cover
            # if alert type is invalid, ignore alert
            return None

        # parse areas
        areas = translation.get("area", [])
        if not isinstance(areas, list):  # pragma: no cover
            areas = [areas]
        self.parse_alert_areas(alert, areas, areas_desc_map)

        # log the summary
        self.logger.info("Parsed alert: %s", alert.id)
        self.logger.debug(alert)
        self.logger.debug(alert.areas)
        return alert

    def parse_alert_info(self, alert: AlertInfo, data: dict[str, str]) -> None:
        """Parse alert information."""
        alert.onset = self.parse_alert_datetime(data.get("onset", ""))
        alert.expires = self.parse_alert_datetime(data.get("expires", ""))
        alert.certainty = AlertCertainty(data.get("certainty", "").lower())
        alert.severity = AlertSeverity(data.get("severity", "").lower())
        alert.urgency = AlertUrgency(data.get("urgency", "").lower())
        alert.response_type = AlertResponseType(data.get("responseType", "").lower())

    def parse_alert_translations(
        self,
        alert: AlertInfo,
        language: LanguageID,
        data: dict[str, str],
    ) -> None:
        """Parse translatable alert data."""
        alert.event[language] = data.get("event", "").strip()
        alert.headline[language] = data.get("headline", "").strip()
        if data.get("description"):  # pragma: no branch
            alert.description[language] = data.get("description", "").strip()
        if data.get("instruction"):  # pragma: no branch
            alert.instructions[language] = data.get("instruction", "").strip()
        alert.sender_name[language] = data.get("senderName", "").strip()
        alert.web[language] = data.get("web", "").strip()

    def parse_alert_parameters(
        self,
        alert: AlertInfo,
        parameters: list[dict[str, str]],
    ) -> None:
        """Parse alert parameters."""
        for parameter in parameters:
            if parameter.get("valueName") == "awareness_type":
                alert.type = AlertType(
                    parameter.get("value", "").lower().split("; ")[-1].strip(),
                )

    def parse_alert_areas(
        self,
        alert: AlertInfo,
        areas: list[dict[str, list[dict[str, str]] | dict[str, str] | str]],
        areas_desc_map: dict[str, str],
    ) -> None:
        """Parse alert areas."""
        for area in areas:
            if "geocode" not in area:  # pragma: no cover
                if "areaDesc" in area:
                    self.logger.warning("Geocode not present, using description")
                    description = area.get("areaDesc")
                    if isinstance(description, str) and description in areas_desc_map:
                        alert.areas.add(areas_desc_map[description])
                continue

            geocode = area.get("geocode")
            if isinstance(geocode, list):  # pragma: no branch
                for geoitem in geocode:
                    if geoitem.get("valueName") == "EMMA_ID":
                        alert.areas.add(geoitem.get("value", "").strip())
            elif (  # pragma: no cover
                isinstance(geocode, dict) and geocode.get("valueName") == "EMMA_ID"
            ):
                alert.areas.add(geocode.get("value", "").strip())
