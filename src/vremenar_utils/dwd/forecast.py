"""DWD MOSMIX utils."""
from tempfile import NamedTemporaryFile

from vremenar_utils.cli.common import CountryID

from ..cli.logging import Logger
from ..database.redis import redis
from ..database.stations import load_stations

from .database import BatchedMosmix
from .mosmix import download
from .parsers import MOSMIXParserFast
from .stations import load_stations as load_local_stations


async def process_mosmix(
    logger: Logger,
    local_source: bool | None = False,
    local_stations: bool | None = False,
) -> str:
    """Cache DWD MOSMIX data."""
    # load stations to use
    station_ids: list[str] = []
    if local_stations:
        logger.info('Loading DWD MOSMIX station IDs from the local database')
        station_ids = [key for key in load_local_stations().keys()]
    else:
        stations_dict = await load_stations(CountryID.Germany)
        station_ids = list(stations_dict.keys())

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix='.kmz', prefix='DWD_MOSMIX_')
        await download(logger, temporary_file)

    parser = MOSMIXParserFast(
        logger, temporary_file.name if temporary_file else 'MOSMIX_S_LATEST_240.kmz'
    )
    async with redis.client() as db:
        async with BatchedMosmix(db) as batch:
            for record in parser.parse(station_ids):
                await batch.add(record)
    if temporary_file:
        temporary_file.close()

    return 'Processed all placemarks'
