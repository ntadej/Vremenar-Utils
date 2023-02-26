"""DWD current weather utils."""
from csv import reader
from io import BytesIO, TextIOWrapper
from pathlib import Path
from pkgutil import get_data
from tempfile import NamedTemporaryFile
from typing import IO

from httpx import AsyncClient

from vremenar_utils.cli.logging import Logger
from vremenar_utils.database.redis import redis

from .database import BatchedCurrentWeather
from .parsers import CurrentObservationsParser


def current_stations() -> list[str]:
    """Get a list of supported DWD stations."""
    data = get_data("vremenar_utils", "data/stations/DWD.current.csv")
    if not data:  # pragma: no cover
        return []

    stations: list[str] = []
    bytes_io = BytesIO(data)
    with TextIOWrapper(bytes_io, encoding="utf-8") as csvfile:
        csv = reader(csvfile)
        for row in csv:
            stations.append(row[0])
    return stations


async def download_current_weather(
    logger: Logger,
    url: str,
    temporary_file: IO[bytes],
) -> None:
    """Download the mosmix data."""
    logger.info("Downloading current weather data from %s ...", url)
    logger.info("Temporary file: %s", temporary_file.name)
    client = AsyncClient()
    async with client.stream("GET", url) as r:
        async for chunk in r.aiter_raw():
            temporary_file.write(chunk)
    temporary_file.flush()
    await client.aclose()


async def current_weather(logger: Logger, test_mode: bool = False) -> None:
    """Cache DWD current weather data."""
    stations: list[str] = current_stations()
    if test_mode:  # pragma: no cover
        stations = [stations[0], stations[-1]]

    async with redis.client() as db:
        async with BatchedCurrentWeather(db) as batch:
            for sid in stations:
                station_id = sid
                if len(station_id) == 4:
                    station_id += "_"
                url = (
                    "https://opendata.dwd.de/weather/weather_reports/poi/"
                    f"{station_id}-BEOB.csv"
                )

                temporary_file = NamedTemporaryFile(
                    suffix=".csv",
                    prefix=f"DWD_CURRENT_{station_id}",
                )
                await download_current_weather(logger, url, temporary_file)

                try:
                    parser = CurrentObservationsParser(
                        logger,
                        Path(temporary_file.name),
                    )
                    for record in parser.parse():
                        await batch.add(record)
                finally:
                    temporary_file.close()

                logger.info(
                    "Done getting current weather data for station %s",
                    station_id,
                )
