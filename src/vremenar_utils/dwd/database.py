"""DWD database utilities."""
from ..cli.common import CountryID
from ..cli.logging import Logger
from ..database.stations import store_station, validate_stations

from .stations import load_stations, zoom_level_conversion


async def store_stations(logger: Logger) -> None:
    """Store DWD stations to redis."""
    country = CountryID.Germany
    stations = load_stations()

    for id, station in stations.items():
        station_out = {
            'id': id,
            'name': station['name'],
            'latitude': station['lat'],
            'longitude': station['lon'],
            'altitude': station['altitude'],
            'zoom_level': zoom_level_conversion(
                str(station['type']), float(station['admin'])
            ),
            'forecast_only': int(not station['has_reports']),
        }

        station_metadata = {
            'status': station['status'],
            'DWD_ID': station['dwd_station_id'],
        }

        await store_station(country, station_out, station_metadata)

    logger.info('%d stations stored', len(stations))

    removed = await validate_stations(country, set(stations.keys()))

    logger.info('%d obsolete stations removed', removed)
