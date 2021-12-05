"""DWD stations utils."""
from csv import reader, writer
from deta import Deta  # type: ignore
from httpx import get as httpx_get, RequestError, HTTPStatusError
from io import BytesIO, TextIOWrapper
from operator import itemgetter
from os import getenv
from pathlib import Path
from pkgutil import get_data
from shapely.geometry import Point  # type: ignore
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

from ..database.utils import BatchedPut
from ..geo.shapes import load_shape, inside_shape
from .mosmix import MOSMIXParserFast, download

DWD_CACHE_DIR: Path = Path.cwd() / '.cache/dwd'
VREMENAR_STATIONS_ENDPOINT: str = 'https://1pjjgy.deta.dev/mosmix'


def load_stations() -> Dict[str, Dict[str, Any]]:
    """Get a dictionary of supported DWD stations."""
    stations: Dict[str, Dict[str, Any]] = {}
    data = get_data('vremenar_utils', 'data/stations/DWD.csv')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as csvfile:
            csv = reader(csvfile, dialect='excel')
            for row in csv:
                stations[row[0]] = {
                    'wmo_station_id': row[0],
                    'dwd_station_id': row[1],
                    'station_name': row[2],
                    'name': row[3],
                    'lat': float(row[4]),
                    'lon': float(row[5]),
                    'type': row[6],
                    'admin': row[7],
                    'status': row[8],
                }
    return stations


def get_stations_mosmix() -> List[str]:
    """Get DWD MOSMIX station IDs from a dedicated microservice."""
    print(f"Getting DWD MOSMIX station IDs from '{VREMENAR_STATIONS_ENDPOINT}'")
    headers = {
        'Content-Type': 'application/json',
        'X-API-Key': getenv('VREMENAR_DWD_STATIONS_KEY', ''),
    }
    try:
        response = httpx_get(VREMENAR_STATIONS_ENDPOINT, headers=headers)
    except RequestError as e:
        print(f'An error occurred: {str(e)}')
        return []
    except HTTPStatusError as e:
        print(f'Error response: {e.response.status_code}')
        return []

    if response.is_error:
        print(f'Error response: {response.status_code}')
        return []

    return response.json()


def process_mosmix_stations(
    output: str,
    output_new: str,
    disable_database: Optional[bool] = False,
    local_source: Optional[bool] = False,
) -> None:
    """Load DWD MOSMIX stations."""
    old_stations = load_stations()
    exceptions = ['10015', 'E5344', '10033', 'A201', '10044', '10097', '10181', 'E043']

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix='.kmz', prefix='DWD_MOSMIX_')
        download(temporary_file)

    meta_keys = ['name', 'type', 'admin', 'status']

    parser = MOSMIXParserFast(
        path=temporary_file.name if temporary_file else 'MOSMIX_S_LATEST_240.kmz',
        url=None,
    )
    stations: List[Dict[str, Any]] = []
    for station in parser.stations():
        if 'SWIS-PUNKT' in station['station_name']:
            continue
        id = station['wmo_station_id']
        if id in old_stations.keys():
            station.update({key: old_stations[id][key] for key in meta_keys})
        else:
            station.update({key: '' for key in meta_keys})
        station['dwd_station_id'] = (
            str(int(station['dwd_station_id'])) if station['dwd_station_id'] else ''
        )
        stations.append(station)
    if temporary_file:
        temporary_file.close()

    # sort
    stations = sorted(stations, key=itemgetter('wmo_station_id', 'name', 'lon', 'lat'))
    stations_keys: List[str] = []
    stations_db: List[Dict[str, Any]] = []

    shape, shape_buffered = load_shape('Germany')
    with open(output, 'w', newline='') as csvfile, open(
        output_new, 'w', newline=''
    ) as csvfile_new:
        csv = writer(csvfile)
        csv_new = writer(csvfile_new)
        keys = [
            'wmo_station_id',
            'dwd_station_id',
            'station_name',
            'name',
            'lat',
            'lon',
            'type',
            'admin',
            'status',
        ]
        for station in stations:
            point = Point(station['lon'], station['lat'])
            valid = inside_shape(point, shape_buffered)
            if station['wmo_station_id'] in exceptions:
                valid = True
            if not valid:
                continue

            if station['name']:
                csv.writerow([station[key] for key in keys])
                stations_keys.append(station['wmo_station_id'])
                stations_db.append({key: station[key] for key in keys})
            else:
                csv_new.writerow([station[key] for key in keys])

    print(f'Processed {len(stations_db)} stations!')

    if not disable_database:
        deta = Deta()
        db = deta.Base('dwd_stations')

        with BatchedPut(db) as batch:
            for station in stations_db:
                batch.put(station, station['wmo_station_id'])

        print('Updated database!')

        removed: List[str] = []

        last_item = None
        total_count = 0
        while True:
            result = db.fetch(last=last_item)
            total_count += result.count
            for item in result.items:
                if item['key'] not in stations_keys:
                    db.delete(item['key'])
                    removed.append(item['key'])
            if not result.last:
                break
            last_item = result.last

        print(f'Read {total_count} stations from the database')
        print(f'Deleted {len(removed)} obsolete stations')
        print(f'Total {total_count - len(removed)} stations')
