"""DWD MOSMIX utils."""

from pathlib import Path
from tempfile import NamedTemporaryFile

from vremenar_utils.cli.common import CountryID
from vremenar_utils.cli.logging import Logger
from vremenar_utils.database.redis import redis
from vremenar_utils.database.stations import load_stations

from .database import BatchedMosmix
from .mosmix import download
from .parsers import MOSMIXParserFast
from .stations import load_stations as load_local_stations


async def process_mosmix(
    logger: Logger,
    local_source: bool | None = False,
    local_stations: bool | None = False,
) -> None:
    """Cache DWD MOSMIX data."""
    # load stations to use
    station_ids: list[str] = []
    if local_stations:
        logger.info("Loading DWD MOSMIX station IDs from the local database")
        station_ids = list(load_local_stations().keys())
    else:
        stations_dict = await load_stations(CountryID.Germany)
        station_ids = list(stations_dict.keys())

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix=".kmz", prefix="DWD_MOSMIX_")
        await download(logger, temporary_file)

    file_path = Path(
        temporary_file.name if temporary_file else "MOSMIX_S_LATEST_240.kmz",
    )
    parser = MOSMIXParserFast(logger, file_path)
    async with redis.client() as db, BatchedMosmix(db) as batch:  # pragma: no branch
        for record in parser.parse(station_ids):
            await batch.add(record)
    if temporary_file:  # pragma: no branch
        temporary_file.close()

    logger.info("Processed all placemarks")
