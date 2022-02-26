"""DWD stations utils."""
from csv import reader, writer
from io import BytesIO, TextIOWrapper
from logging import Logger
from operator import itemgetter
from pkgutil import get_data
from shapely.geometry import Point  # type: ignore
from tempfile import NamedTemporaryFile
from typing import Any, Optional, TextIO, Union

from ..geo.shapes import load_shape, inside_shape
from .mosmix import MOSMIXParserFast, download

DWD_STATION_KEYS = [
    'wmo_station_id',
    'dwd_station_id',
    'has_reports',
    'station_name',
    'name',
    'lat',
    'lon',
    'altitude',
    'type',
    'admin',
    'status',
]


def load_stations() -> dict[str, dict[str, Union[str, int, float]]]:
    """Get a dictionary of supported DWD stations."""
    stations: dict[str, dict[str, Union[str, int, float]]] = {}
    data = get_data('vremenar_utils', 'data/stations/DWD.csv')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as csv_file:
            stations = load_stations_from_csv(csv_file)
    return stations


def load_stations_from_csv(
    csv_file: TextIO,
) -> dict[str, dict[str, Union[str, int, float]]]:
    """Get a dictionary of supported DWD stations from a CSV file."""
    stations: dict[str, dict[str, Union[str, int, float]]] = {}

    csv = reader(csv_file, dialect='excel')
    for row in csv:
        station: dict[str, Union[str, int, float]] = {
            key: row[index] for index, key in enumerate(DWD_STATION_KEYS)
        }
        station['has_reports'] = int(station['has_reports'])
        station['lat'] = float(station['lat'])
        station['lon'] = float(station['lon'])
        station['altitude'] = float(station['altitude'])

        stations[row[0]] = station
    return stations


def load_stations_with_reports() -> list[str]:
    """Get a list of DWD stations that have current weather reports available."""
    stations: list[str] = []
    data = get_data('vremenar_utils', 'data/stations/DWD.current.csv')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as csvfile:
            csv = reader(csvfile, dialect='excel')
            for row in csv:
                stations.append(row[0])
    return stations


def load_stations_included() -> list[str]:
    """Get a list of DWD stations that should always be included."""
    stations: list[str] = []
    data = get_data('vremenar_utils', 'data/stations/DWD.include.csv')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as csvfile:
            csv = reader(csvfile, dialect='excel')
            for row in csv:
                stations.append(row[0])
    return stations


def load_stations_ignored() -> list[str]:
    """Get a list of DWD stations that should be ignored."""
    stations: list[str] = []
    data = get_data('vremenar_utils', 'data/stations/DWD.ignore.csv')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as csvfile:
            csv = reader(csvfile, dialect='excel')
            for row in csv:
                stations.append(row[0])
    return stations


def process_mosmix_stations(
    logger: Logger,
    output: str,
    output_new: str,
    local_source: Optional[bool] = False,
) -> None:
    """Load DWD MOSMIX stations."""
    old_stations = load_stations()
    stations_with_reports = load_stations_with_reports()
    stations_included = load_stations_included()
    stations_ignored = load_stations_ignored()

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix='.kmz', prefix='DWD_MOSMIX_')
        download(logger, temporary_file)

    meta_keys = ['name', 'type', 'admin', 'status']

    parser = MOSMIXParserFast(
        path=temporary_file.name if temporary_file else 'MOSMIX_S_LATEST_240.kmz',
        url=None,
    )
    stations: list[dict[str, Any]] = []
    for station in parser.stations():
        if 'SWIS-PUNKT' in station['station_name']:
            continue
        id = station['wmo_station_id']
        station['has_reports'] = int(id in stations_with_reports)
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
    stations_keys: list[str] = []
    stations_db: list[dict[str, Any]] = []

    shape, shape_buffered = load_shape('Germany')
    with open(output, 'w', newline='') as csvfile, open(
        output_new, 'w', newline=''
    ) as csvfile_new:
        csv = writer(csvfile)
        csv_new = writer(csvfile_new)
        for station in stations:
            point = Point(station['lon'], station['lat'])
            valid = inside_shape(point, shape_buffered)
            if station['wmo_station_id'] in stations_ignored:
                valid = False
            if station['wmo_station_id'] in stations_included:
                valid = True
            if not valid:
                continue

            if station['name']:
                csv.writerow([station[key] for key in DWD_STATION_KEYS])
                stations_keys.append(station['wmo_station_id'])
                stations_db.append({key: station[key] for key in DWD_STATION_KEYS})
            else:
                csv_new.writerow([station[key] for key in DWD_STATION_KEYS])

    logger.info(f'Processed {len(stations_db)} stations!')
