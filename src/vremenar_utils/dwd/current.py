"""DWD current weather utils."""
from brightsky.parsers import CurrentObservationsParser  # type: ignore
from csv import reader
from deta import Deta  # type: ignore
from io import BytesIO, TextIOWrapper
from json import dump
from pathlib import Path
from pkgutil import get_data
from typing import Any, Dict, List, Optional

from ..database.utils import BatchedPut

DwdRecord = Dict[str, Any]
NaN = float('nan')

DWD_CACHE_DIR: Path = Path.cwd() / '.cache/dwd'


def current_stations() -> List[str]:
    """Get a list of supported DWD stations."""
    stations: List[str] = []
    data = get_data('vremenar_utils', 'data/stations/DWD.current.csv')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as csvfile:
            csv = reader(csvfile)
            for row in csv:
                stations.append(row[0])
    return stations


def current_weather(
    disable_cache: Optional[bool] = False,
    disable_database: Optional[bool] = False,
    use_tmp: Optional[bool] = False,
) -> None:
    """Cache DWD current weather data."""
    db = None
    if not disable_database:
        deta = Deta()
        db = deta.Base('dwd_current')

    stations: List[str] = current_stations()
    records: List[DwdRecord] = []

    with BatchedPut(db) as batch:
        for station_id in stations:
            if len(station_id) == 4:
                station_id += '_'
            url = (
                'https://opendata.dwd.de/weather/weather_reports/poi/'
                f'{station_id}-BEOB.csv'
            )
            path = None
            if use_tmp:
                path = f'/tmp/{station_id}.csv'

            print(url)

            parser = CurrentObservationsParser(url=url, path=path)
            parser.download()
            for record in parser.parse(lat=NaN, lon=NaN, height=NaN, station_name=''):
                # update timestamps
                record['time'] = record['timestamp'].strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
                record['timestamp'] = str(int(record['timestamp'].timestamp())) + '000'
                # cleanup
                del record['lat']
                del record['lon']
                del record['height']
                del record['station_name']
                # store
                if not disable_database:
                    key: str = record['wmo_station_id']
                    batch.put(record, key)
                if not disable_cache:
                    records.append(record)
            parser.cleanup()  # If you wish to delete any downloaded files

    if not disable_cache:
        DWD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(DWD_CACHE_DIR / 'CURRENT.json', 'w') as file:
            dump(records, file)
