"""DWD stations utils."""
from csv import reader, writer
from io import BytesIO, TextIOWrapper
from operator import itemgetter
from pkgutil import get_data
from shapely.geometry import Point  # type: ignore
from tempfile import NamedTemporaryFile
from typing import Any, TextIO

from ..cli.logging import Logger
from ..geo.shapes import load_shape, inside_shape
from .mosmix import MOSMIXParserFast, download

DWD_STATION_KEYS = [
    'station_id',
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


def zoom_level_conversion(location_type: str, admin_level: float) -> float:
    """DWD zoom level conversions."""
    # location_type: 'city', 'town', 'village', 'suburb', 'hamlet', 'isolated',
    #                'airport', 'special'
    # admin_level: '4', '6', '8', '9', '10'
    if admin_level >= 10:
        return 10.35
    if admin_level >= 9:
        return 9.9
    if admin_level >= 8:
        if location_type in ['town']:
            return 8.5
        if location_type in ['village', 'suburb']:
            return 9.1
        return 9.5
    return 7.5


def load_stations() -> dict[str, dict[str, str | int | float]]:
    """Get a dictionary of supported DWD stations."""
    data = get_data('vremenar_utils', 'data/stations/DWD.csv')
    if not data:  # pragma: no cover
        return {}

    stations: dict[str, dict[str, str | int | float]] = {}
    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding='utf-8') as csv_file:
        stations = load_stations_from_csv(csv_file)
    return stations


def load_stations_from_csv(
    csv_file: TextIO,
) -> dict[str, dict[str, str | int | float]]:
    """Get a dictionary of supported DWD stations from a CSV file."""
    stations: dict[str, dict[str, str | int | float]] = {}

    csv = reader(csv_file, dialect='excel')
    for row in csv:
        station: dict[str, str | int | float] = {
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
    data = get_data('vremenar_utils', 'data/stations/DWD.current.csv')
    if not data:  # pragma: no cover
        return []

    stations: list[str] = []
    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding='utf-8') as csvfile:
        csv = reader(csvfile, dialect='excel')
        for row in csv:
            stations.append(row[0])
    return stations


def load_stations_included() -> list[str]:
    """Get a list of DWD stations that should always be included."""
    data = get_data('vremenar_utils', 'data/stations/DWD.include.csv')
    if not data:  # pragma: no cover
        return []

    stations: list[str] = []
    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding='utf-8') as csvfile:
        csv = reader(csvfile, dialect='excel')
        for row in csv:
            stations.append(row[0])
    return stations


def load_stations_ignored() -> list[str]:
    """Get a list of DWD stations that should be ignored."""
    data = get_data('vremenar_utils', 'data/stations/DWD.ignore.csv')
    if not data:  # pragma: no cover
        return []

    stations: list[str] = []
    bytes_data = BytesIO(data)
    with TextIOWrapper(bytes_data, encoding='utf-8') as csvfile:
        csv = reader(csvfile, dialect='excel')
        for row in csv:
            stations.append(row[0])
    return stations


async def process_mosmix_stations(
    logger: Logger,
    output: str,
    output_new: str,
    local_source: bool | None = False,
) -> None:
    """Load DWD MOSMIX stations."""
    old_stations = load_stations()
    stations_with_reports = load_stations_with_reports()
    stations_included = load_stations_included()
    stations_ignored = load_stations_ignored()

    temporary_file = None
    if not local_source:
        temporary_file = NamedTemporaryFile(suffix='.kmz', prefix='DWD_MOSMIX_')
        await download(logger, temporary_file)

    meta_keys = ['name', 'type', 'admin', 'status']

    parser = MOSMIXParserFast(
        path=temporary_file.name if temporary_file else 'MOSMIX_S_LATEST_240.kmz',
        url=None,
    )
    stations: list[dict[str, Any]] = []
    for station in parser.stations():
        if 'SWIS-PUNKT' in station['station_name']:
            continue
        station_id = station['station_id']
        station['has_reports'] = int(station_id in stations_with_reports)
        if station_id in old_stations.keys():
            station.update({key: old_stations[station_id][key] for key in meta_keys})
        else:
            station.update({key: '' for key in meta_keys})
        station['dwd_station_id'] = (
            str(int(station['dwd_station_id'])) if station['dwd_station_id'] else ''
        )
        stations.append(station)
    if temporary_file:
        temporary_file.close()

    # sort
    stations = sorted(stations, key=itemgetter('station_id', 'name', 'lon', 'lat'))
    processed = _write_mosmix_stations(
        stations, stations_ignored, stations_included, output, output_new
    )

    logger.info(f'Processed {processed} stations!')


def _write_mosmix_stations(
    stations: list[dict[str, Any]],
    stations_ignored: list[str],
    stations_included: list[str],
    output: str,
    output_new: str,
) -> int:
    stations_keys: list[str] = []

    shape, shape_buffered = load_shape('Germany')
    with open(output, 'w', newline='') as csvfile, open(
        output_new, 'w', newline=''
    ) as csvfile_new:
        csv = writer(csvfile)
        csv_new = writer(csvfile_new)
        for station in stations:
            point = Point(station['lon'], station['lat'])
            valid = inside_shape(point, shape_buffered)
            if station['station_id'] in stations_ignored:
                valid = False
            if station['station_id'] in stations_included:
                valid = True
            if not valid:
                continue

            if station['name']:
                csv.writerow([station[key] for key in DWD_STATION_KEYS])
                stations_keys.append(station['station_id'])
            else:
                csv_new.writerow([station[key] for key in DWD_STATION_KEYS])

    return len(stations_keys)
