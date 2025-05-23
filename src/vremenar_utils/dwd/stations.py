"""DWD stations utils."""

from __future__ import annotations

from csv import reader, writer
from io import BytesIO, TextIOWrapper
from operator import itemgetter
from pathlib import Path
from pkgutil import get_data
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, TextIO

from shapely.geometry import Point  # type: ignore[import-untyped]

from vremenar_utils.geo.shapes import inside_shape, load_shape

from .mosmix import download
from .parsers import MOSMIXParserFast

if TYPE_CHECKING:
    from vremenar_utils.cli.logging import Logger

DWD_STATION_KEYS = [
    "station_id",
    "dwd_station_id",
    "has_reports",
    "station_name",
    "name",
    "lat",
    "lon",
    "altitude",
    "type",
    "admin",
    "status",
]


def zoom_level_conversion(location_type: str, admin_level: float) -> float:
    """DWD zoom level conversions."""
    # location_type: 'city', 'town', 'village', 'suburb', 'hamlet', 'isolated',
    #                'airport', 'special'
    # admin_level: '4', '6', '8', '9', '10'
    if admin_level >= 10:
        return 10.35
    if admin_level >= 9:
        return 9.9
    if admin_level >= 8:
        if location_type in ["town"]:
            return 8.5
        if location_type in ["village", "suburb"]:
            return 9.1
        return 9.5
    return 7.5


def load_stations() -> dict[str, dict[str, str | int | float]]:
    """Get a dictionary of supported DWD stations."""
    data = get_data("vremenar_utils", "data/stations/DWD.csv")
    if not data:  # pragma: no cover
        return {}

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as csv_file:
        return load_stations_from_csv(csv_file)


def load_stations_from_csv(
    csv_file: TextIO,
) -> dict[str, dict[str, str | int | float]]:
    """Get a dictionary of supported DWD stations from a CSV file."""
    stations: dict[str, dict[str, str | int | float]] = {}

    csv = reader(csv_file, dialect="excel")
    for row in csv:
        station: dict[str, str | int | float] = {
            key: row[index] for index, key in enumerate(DWD_STATION_KEYS)
        }
        station["has_reports"] = int(station["has_reports"])
        station["lat"] = float(station["lat"])
        station["lon"] = float(station["lon"])
        station["altitude"] = float(station["altitude"])

        stations[row[0]] = station
    return stations


def load_stations_with_reports() -> list[str]:
    """Get a list of DWD stations that have current weather reports available."""
    data = get_data("vremenar_utils", "data/stations/DWD.current.csv")
    if not data:  # pragma: no cover
        return []

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as csvfile:
        csv = reader(csvfile, dialect="excel")
        return [row[0] for row in csv]


def load_stations_included() -> list[str]:
    """Get a list of DWD stations that should always be included."""
    data = get_data("vremenar_utils", "data/stations/DWD.include.csv")
    if not data:  # pragma: no cover
        return []

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as csvfile:
        csv = reader(csvfile, dialect="excel")
        return [row[0] for row in csv]


def load_stations_ignored() -> list[str]:
    """Get a list of DWD stations that should be ignored."""
    data = get_data("vremenar_utils", "data/stations/DWD.ignore.csv")
    if not data:  # pragma: no cover
        return []

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as csvfile:
        csv = reader(csvfile, dialect="excel")
        return [row[0] for row in csv]


async def process_mosmix_stations(
    logger: Logger,
    output: Path,
    output_new: Path,
    local_source: bool | None = False,
) -> None:
    """Load DWD MOSMIX stations."""
    old_stations = load_stations()
    stations_with_reports = load_stations_with_reports()
    stations_included = load_stations_included()
    stations_ignored = load_stations_ignored()

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix=".kmz", prefix="DWD_MOSMIX_")  # noqa: SIM115
        await download(logger, temporary_file)

    meta_keys = ["name", "type", "admin", "status"]

    file_path = Path(
        temporary_file.name if temporary_file else "MOSMIX_S_LATEST_240.kmz",
    )
    parser = MOSMIXParserFast(logger, file_path)
    stations: list[dict[str, str | int | float | None]] = []
    for station in parser.stations():
        if (
            isinstance(station["station_name"], str)
            and "SWIS-PUNKT" in station["station_name"]
        ):
            continue
        station_id = str(station["station_id"])
        station["has_reports"] = int(station_id in stations_with_reports)
        if station_id in old_stations:
            station.update({key: old_stations[station_id][key] for key in meta_keys})
        else:
            station.update(dict.fromkeys(meta_keys, ""))
        station["dwd_station_id"] = (
            str(int(station["dwd_station_id"])) if station["dwd_station_id"] else ""
        )
        stations.append(station)
    if temporary_file:
        temporary_file.close()

    # sort
    stations = sorted(stations, key=itemgetter("station_id", "name", "lon", "lat"))
    processed = _write_mosmix_stations(
        stations,
        stations_ignored,
        stations_included,
        output,
        output_new,
    )

    logger.info("Processed %d stations", processed)


def _write_mosmix_stations(
    stations: list[dict[str, str | int | float | None]],
    stations_ignored: list[str],
    stations_included: list[str],
    output: Path,
    output_new: Path,
) -> int:
    stations_keys: list[str] = []

    _, shape_buffered = load_shape("Germany")
    with (
        output.open("w", newline="") as csvfile,
        output_new.open(
            "w",
            newline="",
        ) as csvfile_new,
    ):
        csv = writer(csvfile)
        csv_new = writer(csvfile_new)
        for station in stations:
            station_id = str(station["station_id"])
            point = Point(station["lon"], station["lat"])
            valid = inside_shape(point, shape_buffered)
            if station_id in stations_ignored:
                valid = False
            if station_id in stations_included:
                valid = True
            if not valid:
                continue

            if station["name"]:
                csv.writerow([station[key] for key in DWD_STATION_KEYS])
                stations_keys.append(station_id)
            else:
                csv_new.writerow([station[key] for key in DWD_STATION_KEYS])

    return len(stations_keys)
