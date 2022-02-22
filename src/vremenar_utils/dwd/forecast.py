"""DWD MOSMIX utils."""
from datetime import datetime
from deta import Deta  # type: ignore
from json import dumps
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional, TextIO

from ..database.deta import BatchedPut
from .mosmix import MOSMIXParserFast, download
from .stations import load_stations, get_stations_mosmix

DWD_TMP_DIR: Path = Path.cwd() / '.cache/tmp'
DWD_CACHE_DIR: Path = Path.cwd() / '.cache/dwd'


def output_name(date: datetime) -> str:
    """Get MOSMIX cache file name."""
    return date.strftime('MOSMIX:%Y-%m-%dT%H:%M:%S') + 'Z'


def open_file(source: str) -> TextIO:
    """Open cache file."""
    file = open(DWD_TMP_DIR / f'{source}.json', 'w')
    print('[', file=file)
    return file


def close_file(file: TextIO) -> None:
    """Close cache file."""
    print(']', file=file)
    file.close()


def process_mosmix(
    job: Optional[int] = 0,
    disable_cache: Optional[bool] = False,
    disable_database: Optional[bool] = False,
    local_source: Optional[bool] = False,
    local_stations: Optional[bool] = False,
) -> str:
    """Cache DWD MOSMIX data."""
    if not disable_cache:
        DWD_TMP_DIR.mkdir(parents=True, exist_ok=True)

    data: dict[str, TextIO] = {}

    # load stations to use
    station_ids: list[str] = []
    if not local_stations:
        station_ids = get_stations_mosmix()
    if not station_ids:
        print('Loading DWD MOSMIX station IDs from the local database')
        station_ids = [key for key in load_stations().keys()]

    db = None
    if not disable_database:
        deta = Deta()
        db = deta.Base('dwd_mosmix')

    # setup batching if needed
    job_size: int = 250
    min_entry: int = 0
    max_entry: int = 0
    message: str = 'Processed all placemarks'
    if job and job > 0:
        min_entry = (job - 1) * job_size
        max_entry = job * job_size
        message = f'Processed placemarks from #{min_entry+1} to #{max_entry}'
        print(f'Processing placemarks from #{min_entry+1} to #{max_entry}')

    with BatchedPut(db) as batch:
        temporary_file = None
        if not local_source:
            temporary_file = NamedTemporaryFile(suffix='.kmz', prefix='DWD_MOSMIX_')
            download(temporary_file)

        parser = MOSMIXParserFast(
            path=temporary_file.name if temporary_file else 'MOSMIX_S_LATEST_240.kmz',
            url=None,
        )
        for record in parser.parse(station_ids, min_entry, max_entry):
            source: str = output_name(record['timestamp'])
            record['timestamp'] = str(int(record['timestamp'].timestamp())) + '000'
            # write to the DB
            if not disable_database:
                key: str = f"{record['timestamp']}_{record['wmo_station_id']}"
                batch.put(record, key)
            # write to the local cache
            if not disable_cache:
                if source not in data:
                    data[source] = open_file(source)
                    data[source].write(dumps(record))
                else:
                    data[source].write(f',\n{dumps(record)}')
        if temporary_file:
            temporary_file.close()

    if not disable_cache:
        for _, file in data.items():
            close_file(file)

        DWD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        for path in DWD_TMP_DIR.iterdir():
            path.rename(DWD_CACHE_DIR / path.name)
        DWD_TMP_DIR.rmdir()

    return message


def cleanup_mosmix() -> None:
    """Cleanup DWD MOSMIX data."""
    DWD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow()
    for path in DWD_CACHE_DIR.glob('MOSMIX*.json'):
        name = path.name.replace('MOSMIX:', '').strip('.json')
        date = datetime.strptime(name, '%Y-%m-%dT%H:%M:%SZ')
        delta = date - now

        if delta.days < -1:
            print(path.name)
            path.unlink()
