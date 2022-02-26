"""MeteoAlarm parsing."""
# Inspired and based on https://github.com/rolfberkenbosch/meteoalert-api
import httpx
from datetime import datetime, timezone
from typing import Optional, Union, cast
from xmltodict import parse  # type: ignore

from ..cli.common import CountryID, LanguageID

from .common import (
    AlertCertainty,
    AlertInfo,
    AlertResponseType,
    AlertSeverity,
    AlertType,
    AlertUrgency,
)

METEOALARM_ATOM_ENDPOINT = (
    'https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-atom-{0}'
)


class MeteoAlarmParser:
    """MeteoAlarm Atom feed parser."""

    def __init__(self, country: CountryID, existing_alerts: set[str]) -> None:
        """Initialize MeteoAlarm parser."""
        self.country: CountryID = country
        self.now: datetime = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.existing_alert_ids: set[str] = existing_alerts
        self.obsolete_alert_ids: set[str] = set()

    def get_new_alerts(self) -> set[tuple[str, str]]:
        """Retrieve new alerts."""
        endpoint = METEOALARM_ATOM_ENDPOINT.format(self.country.value)
        response = httpx.get(endpoint, timeout=10)
        result = response.text

        all_ids = set()
        new_ids = set()

        # Parse the XML response for the alert feed and loop over the entries
        feed_data = parse(result)
        feed = feed_data.get('feed', [])
        entries = feed.get('entry', [])
        for entry in entries if type(entries) is list else [entries]:
            id = entry.get('cap:identifier')
            all_ids.add(id)
            if id in self.existing_alert_ids:
                continue

            expires = self.parse_alert_datetime(entry.get('cap:expires'))
            if expires < self.now:
                continue

            # Get the cap URL for additional alert data
            cap_url = None
            for link in entry.get('link'):
                if 'hub' in link.get('@href'):
                    cap_url = link.get('@href')

            if not cap_url:
                continue

            new_ids.add((id, cap_url))

        # check for alerts that are no longer present
        for id in self.existing_alert_ids:
            if id not in all_ids:
                self.obsolete_alert_ids.add(id)

        return new_ids

    def parse_alert_datetime(self, string: str) -> datetime:
        """Parse alert date/time."""
        return datetime.strptime(string, '%Y-%m-%dT%H:%M:%S%z')

    def parse_cap(self, id: str, url: str) -> Optional[AlertInfo]:
        """Parse CAP."""
        alert = AlertInfo(id)

        # Parse the XML response for the alert information
        response = httpx.get(url, timeout=10)
        result = response.text
        # can be empty
        if not result:
            return None

        result = result[: result.rfind('>') + 1]  # ignore characters after last XML tag

        alert_data = parse(result)
        alert_dict = alert_data.get('alert', {})

        # get the alert data in the supported languages
        translations = alert_dict.get('info', [])

        if isinstance(translations, list):
            for translation in translations:
                try:
                    lang = LanguageID(translation.get('language')[:2])
                except ValueError:
                    continue
                self.parse_alert_info(alert, translation)
                self.parse_alert_translations(alert, lang, translation)
        else:
            try:
                lang = LanguageID(translation.get('language')[:2])
            except ValueError:
                lang = LanguageID.English
            translation = translations
            self.parse_alert_info(alert, translation)
            self.parse_alert_translations(alert, lang, translation)

        # parse parameters
        parameters = translation.get('parameter', [])
        self.parse_alert_parameters(alert, parameters)

        # parse areas
        areas = translation.get('area', [])
        if not isinstance(areas, list):
            areas = [areas]
        self.parse_alert_areas(alert, areas)

        # print the summary
        print(alert)
        print(alert.areas)
        print()
        return alert

    def parse_alert_info(self, alert: AlertInfo, data: dict[str, str]) -> None:
        """Parse alert information."""
        alert.onset = self.parse_alert_datetime(data.get('onset', ''))
        alert.expires = self.parse_alert_datetime(data.get('expires', ''))
        alert.certainty = AlertCertainty(data.get('certainty', '').lower())
        alert.severity = AlertSeverity(data.get('severity', '').lower())
        alert.urgency = AlertUrgency(data.get('urgency', '').lower())
        alert.response_type = AlertResponseType(data.get('responseType', '').lower())

    def parse_alert_translations(
        self, alert: AlertInfo, language: LanguageID, data: dict[str, str]
    ) -> None:
        """Parse translatable alert data."""
        alert.event[language] = data.get('event', '').strip()
        alert.headline[language] = data.get('headline', '').strip()
        if data.get('description'):
            alert.description[language] = data.get('description', '').strip()
        if data.get('instruction'):
            alert.instructions[language] = data.get('instruction', '').strip()
        alert.sender_name[language] = data.get('senderName', '').strip()
        alert.web[language] = data.get('web', '').strip()

    def parse_alert_parameters(
        self, alert: AlertInfo, parameters: list[dict[str, str]]
    ) -> None:
        """Parse alert parameters."""
        for parameter in parameters:
            if parameter.get('valueName') == 'awareness_type':
                alert.type = AlertType(
                    parameter.get('value', '').lower().split('; ')[-1].strip()
                )

    def parse_alert_areas(
        self,
        alert: AlertInfo,
        areas: list[dict[str, Union[list[dict[str, str]], dict[str, str]]]],
    ) -> None:
        """Parse alert areas."""
        for area in areas:
            if isinstance(area.get('geocode', []), list):
                for geocode in cast(list[dict[str, str]], area.get('geocode', [])):
                    if geocode.get('valueName') == 'EMMA_ID':
                        alert.areas.add(geocode.get('value', '').strip())
            else:
                geocode = cast(dict[str, str], area.get('geocode', {}))
                if geocode.get('valueName') == 'EMMA_ID':
                    alert.areas.add(geocode.get('value', '').strip())
