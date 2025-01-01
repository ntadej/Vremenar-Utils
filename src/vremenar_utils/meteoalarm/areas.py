"""MeteoAlarm areas utils."""

from __future__ import annotations

from io import BytesIO, TextIOWrapper
from json import dump, load
from pathlib import Path
from pkgutil import get_data
from tempfile import NamedTemporaryFile
from typing import IO, Any, cast

from httpx import AsyncClient

from vremenar_utils.cli.common import CountryID
from vremenar_utils.cli.logging import Logger, download_bar
from vremenar_utils.database.stations import load_stations, store_station
from vremenar_utils.geo.polygons import point_in_polygon

from . import TIMEOUT
from .common import AlertArea
from .database import store_alerts_areas

SLOVENIA_NAMES = {
    "SI006": "Severovzhodna Slovenija",
    "SI007": "Severozahodna Slovenija",
    "SI008": "Jugozahodna Slovenija",
    "SI009": "Osrednja Slovenija",
    "SI010": "Jugovzhodna Slovenija",
    "SI801": "Obala Slovenije",
}

SLOVENIA_DESCRIPTIONS = {
    "SI006": "Slovenia / North-West",
    "SI007": "Slovenia / North-East",
    "SI008": "Slovenia / South-West",
    "SI009": "Slovenia / Central",
    "SI010": "Slovenia / South-East",
    "SI801": "Slovenia / Sea",
}


def read_meteoalarm_areas() -> dict[str, Any]:
    """Read MeteoAlarm areas."""
    with Path("meteoalarm_geocodes.json").open() as f:
        return cast(dict[str, Any], load(f))


async def download(logger: Logger, temporary_file: IO[bytes]) -> None:
    """Download the MeteoAlarm area data."""
    url = "https://drive.usercontent.google.com/download?id=16s24hYHfYQhKMNcP1hpgQmg13Yb8j0hV&export=download&authuser=0"
    logger.info("Downloading MeteoAlarm area data from %s ...", url)
    logger.debug("Temporary file: %s", temporary_file.name)
    client = AsyncClient()
    async with client.stream("GET", url, timeout=TIMEOUT) as r:
        total = int(r.headers["Content-Length"]) if "Content-Length" in r.headers else 0

        with download_bar(transient=True) as progress:
            task = progress.add_task("", total=total)
            async for chunk in r.aiter_bytes():
                temporary_file.write(chunk)
                progress.update(task, completed=r.num_bytes_downloaded)

    temporary_file.flush()
    await client.aclose()
    logger.debug("Done!")

    temporary_file.seek(0)


async def process_meteoalarm_areas(
    logger: Logger,
    country: CountryID,
    output: Path,
    output_matches: Path,
    local_source: bool,
) -> None:
    """Process MeteoAlarm ares."""
    if not local_source:  # pragma: no branch
        with NamedTemporaryFile(
            suffix=".json",
            prefix="meteoalarm_geocodes",
        ) as temporary_file:
            await download(logger, temporary_file)
            data = load(temporary_file)
    else:  # pragma: no cover
        data = read_meteoalarm_areas()

    areas: list[AlertArea] = []

    for feature in data["features"]:
        properties = feature["properties"]
        if properties["country"].lower() != country.value:
            continue

        coordinates = feature["geometry"]["coordinates"]
        polygons = []
        for polygon_source in coordinates:
            polygon = polygon_source
            while isinstance(polygon, list) and len(polygon) == 1:
                polygon = polygon[0]
            polygons.append(polygon)

        code = properties["code"]

        # name override
        name = properties["name"]
        description = ""
        if country is CountryID.Slovenia:
            name = SLOVENIA_NAMES.get(code, name)
            description = SLOVENIA_DESCRIPTIONS.get(code, description)

        area = AlertArea(code, name, description, polygons)
        logger.info(area)
        areas.append(area)

    await store_alerts_areas(country, areas)

    with output.open("w") as f:
        dump([area.to_dict() for area in areas], f, indent=2)
        f.write("\n")

    logger.info("Total %d areas", len(areas))

    await match_meteoalarm_areas(country, output_matches, areas)


async def match_meteoalarm_areas(
    country: CountryID,
    output: Path,
    areas: list[AlertArea],
) -> None:
    """Match MeteoAlarm areas with weather stations."""
    # load stations
    stations = await load_stations(country)
    # load overries
    overrides: dict[str, str] = {}
    overrides_data = get_data(
        "vremenar_utils",
        f"data/meteoalarm/{country.value}_overrides.json",
    )
    if overrides_data:  # pragma: no branch
        bytes_data = BytesIO(overrides_data)
        with TextIOWrapper(bytes_data, encoding="utf-8") as file:
            overrides = load(file)

    matches: dict[str, str] = {}

    for station_id, station in stations.items():
        if "country" in station and str(station["country"]).lower() != country.value:
            continue

        matches[station_id] = await process_meteoalarm_station(
            country,
            station_id,
            station,
            areas,
            overrides,
        )

    with output.open("w") as f:
        dump(
            dict(sorted(matches.items(), key=lambda item: item[0])),
            f,
            indent=2,
        )
        f.write("\n")


async def process_meteoalarm_station(
    country: CountryID,
    station_id: str,
    station: dict[str, str | int | float],
    areas: list[AlertArea],
    overrides: dict[str, str],
) -> str:
    """Process MeteoAlarm station."""
    label = station["name"]
    coordinate = [float(station["longitude"]), float(station["latitude"])]

    found = False
    area_code = None
    for area in areas:
        for polygon in area.polygons:
            if point_in_polygon(coordinate, polygon):
                area_code = area.code
                found = True
                break
        if found:
            break

    if not found and station_id in overrides:
        area_code = overrides[station_id]

    if not area_code:  # pragma: no cover
        raise ValueError(station_id, label, coordinate)

    # update database
    await store_station(country, {"id": station_id, "alerts_area": area_code})

    return area_code


def load_meteoalarm_areas(country: CountryID) -> list[AlertArea]:
    """Load MeteoAlarm areas from file."""
    data = get_data("vremenar_utils", f"data/meteoalarm/{country.value}.json")
    if not data:  # pragma: no cover
        return []

    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding="utf-8") as file:
        areas_dict = load(file)

    return [AlertArea.from_dict(area_obj) for area_obj in areas_dict]


def build_meteoalarm_area_description_map(areas: list[AlertArea]) -> dict[str, str]:
    """Build MeteoAlarm areas description map."""
    result = {}
    for area in areas:
        if area.description:
            result[area.description] = area.code
    return result
