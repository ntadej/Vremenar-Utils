"""DWD MOSMIX utils."""
from datetime import datetime
from deta import Deta  # type: ignore
from json import dumps
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Optional, TextIO

from ..database.utils import BatchedPut
from .mosmix import MOSMIXParserFast, download

DWD_CACHE_DIR: Path = Path.cwd() / '.cache/dwd'


def output_name(date: datetime) -> str:
    """Get MOSMIX cache file name."""
    return date.strftime('MOSMIX:%Y-%m-%dT%H:%M:%S') + 'Z'


def open_file(source: str) -> TextIO:
    """Open cache file."""
    file = open(DWD_CACHE_DIR / f'{source}.json', 'w')
    print('[', file=file)
    return file


def close_file(file: TextIO) -> None:
    """Close cache file."""
    print(']', file=file)
    file.close()


def process_mosmix(
    disable_cache: Optional[bool] = False,
    disable_database: Optional[bool] = False,
    job: Optional[int] = 0,
) -> str:
    """Cache DWD MOSMIX data."""
    if not disable_cache:
        DWD_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    data: Dict[str, TextIO] = {}

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

    with BatchedPut(db) as batch, NamedTemporaryFile(
        suffix='.kmz', prefix='DWD_MOSMIX_'
    ) as temporary_file:
        download(temporary_file)

        parser = MOSMIXParserFast(path=temporary_file.name, url=None)
        # parser.download()  # Not necessary if you supply a local path
        for record in parser.parse(min_entry, max_entry):
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
        # parser.cleanup()  # If you wish to delete any downloaded files

    if not disable_cache:
        for _, file in data.items():
            close_file(file)

    return message


def cleanup_mosmix() -> None:
    """Cleanup DWD MOSMIX data."""
    now = datetime.utcnow()
    for path in DWD_CACHE_DIR.glob('MOSMIX*.json'):
        name = path.name.replace('MOSMIX:', '').strip('.json')
        date = datetime.strptime(name, '%Y-%m-%dT%H:%M:%SZ')
        delta = date - now

        if delta.days < -1:
            print(path.name)
            path.unlink()
