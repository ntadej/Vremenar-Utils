"""ARSO stations utilities."""

from __future__ import annotations

from csv import reader
from io import BytesIO, TextIOWrapper
from json import load
from pkgutil import get_data
from typing import TextIO


def load_stations() -> dict[str, dict[str, str | int | float]]:
    """Load ARSO stations."""
    data = get_data("vremenar_utils", "data/stations/ARSO.json")
    if not data:  # pragma: no cover
        return {}

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as file:
        stations = load(file)

    output: dict[str, dict[str, str | int | float]] = {}
    for station in stations:
        station["id"] = station["id"].strip("_")
        output[station["id"]] = station
    return output


def load_stations_map() -> dict[str, str]:
    """Load ARSO stations map."""
    data = get_data("vremenar_utils", "data/stations/ARSO.id.csv")
    if not data:  # pragma: no cover
        return {}

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as file:
        return load_stations_map_from_csv(file)


def load_stations_map_from_csv(csv_file: TextIO) -> dict[str, str]:
    """Get a dictionary of supported DWD stations from a CSV file."""
    stations: dict[str, str] = {}

    csv = reader(csv_file, dialect="excel")
    for row in csv:
        stations[row[0]] = row[1]
    return stations


def zoom_level_conversion(zoom_level: int) -> float:
    """Convert zoom levels from ARSO ones."""
    zoom_level_processed = (
        float(zoom_level) + 1.0 if zoom_level == 5 else float(zoom_level)
    )
    zoom_level_processed /= 6
    zoom_epsilon = 0.25
    zoom_level_processed *= 11 - 7.5 - zoom_epsilon
    return 11 - zoom_level_processed - zoom_epsilon
