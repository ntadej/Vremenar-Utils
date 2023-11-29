"""DWD MOSMIX utils."""
from typing import IO

from httpx import AsyncClient

from vremenar_utils.cli.logging import Logger, download_bar

from . import TIMEOUT

DWD_OPEN_DATA: str = "https://opendata.dwd.de"
DWD_MOSMIX_URL: str = (
    f"{DWD_OPEN_DATA}/weather/local_forecasts/mos/MOSMIX_S/"
    "all_stations/kml/MOSMIX_S_LATEST_240.kmz"
)


async def download(logger: Logger, temporary_file: IO[bytes]) -> None:
    """Download the mosmix data."""
    logger.info("Downloading MOSMIX data from %s ...", DWD_MOSMIX_URL)
    logger.debug("Temporary file: %s", temporary_file.name)
    client = AsyncClient()
    async with client.stream("GET", DWD_MOSMIX_URL, timeout=TIMEOUT) as r:
        total = int(r.headers["Content-Length"])

        with download_bar(transient=True) as progress:
            task = progress.add_task("", total=total)
            async for chunk in r.aiter_raw():
                temporary_file.write(chunk)
                progress.update(task, completed=r.num_bytes_downloaded)

    temporary_file.flush()
    await client.aclose()
    logger.debug("Done!")
