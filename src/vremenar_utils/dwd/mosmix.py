"""DWD MOSMIX utils."""
from typing import IO

from httpx import AsyncClient

from vremenar_utils.cli.logging import Logger

DWD_OPEN_DATA: str = "https://opendata.dwd.de"
DWD_MOSMIX_URL: str = (
    f"{DWD_OPEN_DATA}/weather/local_forecasts/mos/MOSMIX_S/"
    "all_stations/kml/MOSMIX_S_LATEST_240.kmz"
)


async def download(logger: Logger, temporary_file: IO[bytes]) -> None:
    """Download the mosmix data."""
    logger.info("Downloading MOSMIX data from %s ...", DWD_MOSMIX_URL)
    logger.info("Temporary file: %s", temporary_file.name)
    client = AsyncClient()
    async with client.stream("GET", DWD_MOSMIX_URL) as r:
        async for chunk in r.aiter_raw():
            temporary_file.write(chunk)
    temporary_file.flush()
    await client.aclose()
    logger.info("Done!")
