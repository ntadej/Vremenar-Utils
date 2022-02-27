"""DWD MOSMIX utils."""
from tempfile import NamedTemporaryFile
from typing import Optional

from vremenar_utils.cli.common import CountryID

from ..cli.logging import Logger
from ..database.redis import redis
from ..database.stations import load_stations

from .database import BatchedMosmix
from .mosmix import MOSMIXParserFast, download
from .stations import load_stations as load_local_stations


async def process_mosmix(
    logger: Logger,
    job: Optional[int] = 0,
    local_source: Optional[bool] = False,
    local_stations: Optional[bool] = False,
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

    # setup batching if needed
    job_size: int = 250
    min_entry: int = 0
    max_entry: int = 0
    message: str = 'Processed all placemarks'
    if job and job > 0:
        min_entry = (job - 1) * job_size
        max_entry = job * job_size
        message = f'Processed placemarks from #{min_entry+1} to #{max_entry}'
        logger.info(f'Processing placemarks from #{min_entry+1} to #{max_entry}')

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix='.kmz', prefix='DWD_MOSMIX_')
        await download(logger, temporary_file)

    parser = MOSMIXParserFast(
        path=temporary_file.name if temporary_file else 'MOSMIX_S_LATEST_240.kmz',
        url=None,
    )
    async with redis.client() as db:
        async with BatchedMosmix(db) as batch:
            for record in parser.parse(station_ids, min_entry, max_entry):
                record['timestamp'] = f"{int(record['timestamp'].timestamp())}000"
                await batch.add(record)
    if temporary_file:
        temporary_file.close()

    return message
