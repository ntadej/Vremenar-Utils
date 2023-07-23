"""Parses of DWD open data."""
# Based on brightsky
# Copyright (c) 2020 Jakob de Maeyer
from __future__ import annotations

import re
from csv import DictReader, reader
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, ClassVar
from zipfile import ZipFile

import httpx
from dateutil import parser as dateparser
from lxml.etree import Element, QName, iterparse  # type: ignore
from parsel import Selector, SelectorList

from vremenar_utils import __version__

from .units import (
    celsius_to_kelvin,
    current_observations_weather_code_to_condition,
    hpa_to_pa,
    km_to_m,
    kmh_to_ms,
    minutes_to_seconds,
    synop_past_weather_code_to_condition,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from vremenar_utils.cli.logging import Logger

DWD_OPEN_DATA: str = "https://opendata.dwd.de"
NS = {
    "dwd": f"{DWD_OPEN_DATA}/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    "kml": "http://www.opengis.net/kml/2.2",
}
HEADERS = {
    "user-agent": f"Vremenar-Utils/{__version__}",
    "referer": "https://vremenar.app",
}


class StationIDConverter:
    """DWD station ID converter."""

    STATION_LIST_URL = (
        "https://www.dwd.de/DE/leistungen/klimadatendeutschland/statliste/"
        "statlex_html.html?view=nasPublication"
    )
    STATION_TYPES: ClassVar[list[str]] = ["SY", "MN"]

    def __init__(self: StationIDConverter, logger: Logger) -> None:
        """Initialize DWD station ID converter."""
        self.logger = logger
        self.dwd_to_wmo: dict[str, str] = {}
        self.wmo_to_dwd: dict[str, str] = {}

        self._update()

    def _update(self: StationIDConverter) -> None:
        self.logger.info("Updating station ID maps")
        response = httpx.get(self.STATION_LIST_URL, headers=HEADERS)
        self._parse_station_list(response.text)

    def _parse_station_list(self: StationIDConverter, html: str) -> None:
        sel = Selector(html)
        station_rows: SelectorList[Selector] = SelectorList()
        for station_type in self.STATION_TYPES:
            station_rows.extend(sel.xpath(f'//tr[td[3][text() = "{station_type}"]]'))
        assert station_rows, "No synoptic stations"
        self.dwd_to_wmo.clear()
        self.wmo_to_dwd.clear()
        for row in station_rows:
            values = row.css("td::text").extract()
            dwd_id = values[1].zfill(5)
            wmo_id = values[3]
            self.dwd_to_wmo[dwd_id] = wmo_id
            self.wmo_to_dwd[wmo_id] = dwd_id
        self.logger.info("Parsed %d station ID mappings", len(station_rows))

    def convert_to_wmo(self: StationIDConverter, dwd_id: str) -> str | None:
        """Convert DWD ID to WMO."""
        return self.dwd_to_wmo.get(dwd_id)

    def convert_to_dwd(self: StationIDConverter, wmo_id: str) -> str | None:
        """Convert WMO ID to DWD."""
        return self.wmo_to_dwd.get(wmo_id)


class Parser:
    """Base parser class."""

    def __init__(
        self: Parser,
        logger: Logger,
        path: Path,
        without_station_id_converter: bool = False,
    ) -> None:
        """Initialize the parser."""
        self.logger = logger
        self.path = path
        self.station_id_converter = (
            StationIDConverter(logger) if not without_station_id_converter else None
        )


class CurrentObservationsParser(Parser):
    """Parser of DWD current observations."""

    ELEMENTS: ClassVar[dict[str, str]] = {
        "cloud_cover_total": "cloud_cover",
        "dew_point_temperature_at_2_meter_above_ground": "dew_point",
        "dry_bulb_temperature_at_2_meter_above_ground": "temperature",
        "horizontal_visibility": "visibility",
        "maximum_wind_speed_last_hour": "wind_gust_speed",
        "mean_wind_direction_during_last_10 min_at_10_meters_above_ground": (
            "wind_direction"
        ),
        "mean_wind_speed_during last_10_min_at_10_meters_above_ground": ("wind_speed"),
        "precipitation_amount_last_hour": "precipitation",
        "present_weather": "condition",
        "pressure_reduced_to_mean_sea_level": "pressure_msl",
        "relative_humidity": "relative_humidity",
        "total_time_of_sunshine_during_last_hour": "sunshine",
    }
    DATE_COLUMN = "surface observations"
    HOUR_COLUMN = "Parameter description"

    CONVERTERS: ClassVar[dict[str, Callable[..., float | str | None]]] = {
        "condition": current_observations_weather_code_to_condition,
        "dew_point": celsius_to_kelvin,
        "pressure_msl": hpa_to_pa,
        "sunshine": minutes_to_seconds,
        "temperature": celsius_to_kelvin,
        "visibility": km_to_m,
        "wind_speed": kmh_to_ms,
        "wind_gust_speed": kmh_to_ms,
    }

    def parse(
        self: CurrentObservationsParser,
    ) -> Iterable[dict[str, str | int | float | None]]:
        """Parse current weather."""
        with self.path.open() as f:
            reader = DictReader(f, delimiter=";")
            wmo_station_id = next(reader)[self.DATE_COLUMN].rstrip("_")
            # Skip row with German header titles
            next(reader)
            for row in reader:
                record = self.parse_row(row)
                yield {
                    "station_id": wmo_station_id,
                    **record,
                }
                break  # only parse first row for now

    def parse_row(
        self: CurrentObservationsParser,
        row: dict[str, str],
    ) -> dict[str, str | int | float | None]:
        """Parse a row of data."""
        record: dict[str, str | int | float | None] = {
            element: (
                None if row[column] == "---" else float(row[column].replace(",", "."))
            )
            for column, element in self.ELEMENTS.items()
        }
        time = datetime.strptime(
            f"{row[self.DATE_COLUMN]} {row[self.HOUR_COLUMN]}",
            "%d.%m.%y %H:%M",
        ).replace(tzinfo=timezone.utc)
        record["timestamp"] = f"{int(time.timestamp())}000"
        self._convert_units(record)
        self._sanitize_record(record)
        return record

    def _convert_units(
        self: CurrentObservationsParser,
        record: dict[str, str | int | float | None],
    ) -> None:
        for element, converter in self.CONVERTERS.items():
            if record[element] is not None:
                record[element] = converter(record[element])

    def _sanitize_record(
        self: CurrentObservationsParser,
        record: dict[str, str | int | float | None],
    ) -> None:
        to_sanitize = {
            "cloud_cover": 100,
            "relative_humidity": 100,
            "sunshine": 3600,
        }

        for key, threshold in to_sanitize.items():
            if key in record and record[key]:
                value = record[key]
                if not isinstance(value, int | float):
                    err = f"'{key}' should be a number"
                    raise ValueError(err)
                if value > threshold:
                    self.logger.warning(
                        "Ignoring unphysical value for '%s': %s",
                        key,
                        value,
                    )
                    record[key] = None


class MOSMIXParserFast(Parser):
    """Custom MOSMIX parser for low memory."""

    ELEMENTS: ClassVar[dict[str, str]] = {
        "DD": "wind_direction",
        "FF": "wind_speed",
        "FX1": "wind_gust_speed",
        "N": "cloud_cover",
        "PPPP": "pressure_msl",
        "RR1c": "precipitation",
        "SunD1": "sunshine",
        "Td": "dew_point",
        "TTT": "temperature",
        "VV": "visibility",
        "ww": "condition",
    }

    def parse(
        self: MOSMIXParserFast,
        station_ids: list[str],
    ) -> Iterable[dict[str, str | int | float | None]]:
        """Parse the file."""
        self.logger.info("Parsing %s", self.path)

        with ZipFile(self.path) as zip_file, zip_file.open(
            zip_file.namelist()[0],
        ) as file:
            timestamps: list[str] = []
            accepted_timestamps: list[str] = []
            placemark = 0
            source = ""
            for _, elem in iterparse(file):
                tag = QName(elem.tag).localname

                if tag == "ProductID":
                    source += elem.text + ":"
                    self._clear_element(elem)
                elif tag == "IssueTime":
                    source += elem.text
                    self._clear_element(elem)
                elif tag == "ForecastTimeSteps":
                    timestamps_raw = [
                        dateparser.parse(r.text).replace(tzinfo=timezone.utc)
                        for r in elem.findall("dwd:TimeStep", namespaces=NS)
                    ]
                    timestamps = [f"{int(t.timestamp())}000" for t in timestamps_raw]
                    accepted_timestamps = self._filter_timestamps(timestamps_raw)
                    self._clear_element(elem)

                    self.logger.info(
                        "Got %d timestamps for source %s",
                        len(timestamps),
                        source,
                    )
                    self.logger.info("Using %d timestamps", len(accepted_timestamps))
                elif tag == "Placemark":
                    records = self._parse_station(
                        elem,
                        station_ids,
                        timestamps,
                        accepted_timestamps,
                        source,
                    )
                    self._clear_element(elem)
                    if records:
                        self.logger.info("Processed placemark #%s", placemark + 1)
                        placemark += 1
                        yield from self._sanitize_records(records)

    def stations(
        self: MOSMIXParserFast,
    ) -> Iterable[dict[str, str | int | float | None]]:
        """Parse the file."""
        self.logger.info("Parsing %s", self.path)

        with ZipFile(self.path) as zip_file, zip_file.open(
            zip_file.namelist()[0],
        ) as file:
            for _, elem in iterparse(file):
                tag = QName(elem.tag).localname

                if tag in ["ProductID", "IssueTime", "ForecastTimeSteps"]:
                    self._clear_element(elem)
                elif tag == "Placemark":
                    records = self._parse_station(elem, [], [], [])
                    self._clear_element(elem)
                    if records:
                        yield from records
                    else:  # pragma: no cover
                        continue

    @staticmethod
    def _clear_element(elem: Element) -> None:
        elem.clear()
        # Also eliminate now-empty references from the root node to elem
        for ancestor in elem.xpath("ancestor-or-self::*"):
            while ancestor.getprevious() is not None:
                del ancestor.getparent()[0]

    @staticmethod
    def _filter_timestamps(timestamps: list[datetime]) -> list[str]:
        accepted = []
        now = datetime.now(tz=timezone.utc)
        now = now.replace(minute=0, second=0, microsecond=0)
        if now >= timestamps[0] and now <= timestamps[-1]:  # pragma: no cover
            accepted.append(f"{int(now.timestamp())}000")

        daily = now + timedelta(hours=48 - now.hour)

        # 2 days hourly
        while now < daily:
            now += timedelta(hours=1)
            if now >= timestamps[0] and now <= timestamps[-1]:  # pragma: no cover
                accepted.append(f"{int(now.timestamp())}000")

        # 7 days
        for i in range(28):
            time = daily + timedelta(hours=i * 6)
            if time >= timestamps[0] and time <= timestamps[-1]:  # pragma: no cover
                accepted.append(f"{int(time.timestamp())}000")

        return accepted

    def _parse_station(  # noqa: PLR0913
        self: MOSMIXParserFast,
        station_elem: Element,
        station_ids: list[str],
        timestamps: list[str],
        accepted_timestamps: list[str],
        source: str | None = "",
    ) -> Iterable[dict[str, str | int | float | None]]:
        wmo_station_id = station_elem.find("./kml:name", namespaces=NS).text
        if station_ids and wmo_station_id not in station_ids:
            return []

        station_name = station_elem.find("./kml:description", namespaces=NS).text
        try:
            lon, lat, altitude = station_elem.find(
                "./kml:Point/kml:coordinates",
                namespaces=NS,
            ).text.split(",")
        except AttributeError:  # pragma: no cover
            self.logger.warning(
                "Ignoring station without coordinates, WMO ID '%s', name '%s'",
                wmo_station_id,
                station_name,
            )
            return []

        base_record = {
            "source": source,
            "station_id": wmo_station_id,
        }

        if timestamps:
            records: dict[str, list[str | int | float | None] | list[str]] = {
                "timestamp": timestamps,
            }
            for element, column in self.ELEMENTS.items():
                values_str = station_elem.find(
                    f'./*/dwd:Forecast[@dwd:elementName="{element}"]/dwd:value',
                    namespaces=NS,
                ).text
                converter = getattr(self, f"parse_{column}", float)
                records[column] = [
                    None if row[0] == "-" else converter(row[0])
                    for row in reader(
                        re.sub(r"\s+", "\n", values_str.strip()).splitlines(),
                    )
                ]
                assert len(records[column]) == len(timestamps)

            # Turn dict of lists into list of dicts
            return (
                {**base_record, **dict(zip(records, row, strict=True))}
                for row in zip(*records.values(), strict=True)
                if row[0] in accepted_timestamps
            )

        dwd_station_id = None
        if self.station_id_converter:
            self.station_id_converter.convert_to_dwd(wmo_station_id)
        base_record.update(
            {
                "lat": float(lat),
                "lon": float(lon),
                "altitude": float(altitude),
                "dwd_station_id": dwd_station_id,
                "station_name": station_name,
            },
        )
        return [base_record]

    def _sanitize_records(
        self: MOSMIXParserFast,
        records: Iterable[dict[str, str | int | float | None]],
    ) -> Iterable[dict[str, str | int | float | None]]:
        for r in records:
            if "condition" in r and r["condition"] is not None:
                r["condition"] = synop_past_weather_code_to_condition(
                    int(r["condition"]),
                )

            if "precipitation" in r and r["precipitation"]:
                if not isinstance(r["precipitation"], int | float):
                    err = "'precipitation' should be a number"
                    raise ValueError(err)
                if r["precipitation"] < 0:  # pragma: no cover
                    self.logger.warning("Ignoring negative precipitation value: %s", r)
                    r["precipitation"] = None

            if "wind_direction" in r and r["wind_direction"]:
                if not isinstance(r["wind_direction"], int | float):
                    err = "'wind_direction' should be a number"
                    raise ValueError(err)
                if r["wind_direction"] > 360:  # pragma: no cover
                    self.logger.warning("Fixing out-of-bounds wind direction: %s", r)
                    r["wind_direction"] -= 360

            yield r
