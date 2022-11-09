"""ARSO stations utilities."""
from io import BytesIO, TextIOWrapper
from json import load
from pkgutil import get_data
from typing import Union


def load_stations() -> dict[str, dict[str, Union[str, int, float]]]:
    """Load ARSO stations."""
    data = get_data('vremenar_utils', 'data/stations/ARSO.json')
    if not data:  # pragma: no cover
        return {}

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding='utf-8') as file:
        stations = load(file)

    output: dict[str, dict[str, Union[str, int, float]]] = {}
    for station in stations:
        station['id'] = station['id'].strip('_')
        output[station['id']] = station
    return output


def zoom_level_conversion(zoom_level: float) -> float:
    """Convert zoom levels from ARSO ones."""
    zoom_level = zoom_level + 1.0 if zoom_level == 5.0 else zoom_level
    zoom_level /= 6
    zoom_epsilon = 0.25
    zoom_level *= 11 - 7.5 - zoom_epsilon
    zoom_level = 11 - zoom_level - zoom_epsilon
    return zoom_level
