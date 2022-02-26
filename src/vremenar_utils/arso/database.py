"""ARSO database utilities."""
from .stations import load_stations, zoom_level_conversion

from ..cli.common import CountryID
from ..cli.logging import Logger
from ..database.stations import store_station, validate_stations


async def store_stations(logger: Logger) -> None:
    """Store DWD stations to redis."""
    country = CountryID.Slovenia
    stations = load_stations()

    for id, station in stations.items():
        station_out = {
            'id': id,
            'name': station['title'],
            'latitude': station['latitude'],
            'longitude': station['longitude'],
            'altitude': station['altitude'],
            'zoom_level': zoom_level_conversion(float(station['zoomLevel'])),
            'forecast_only': 0,
        }

        station_metadata = {
            'country': station['country'],
            'region': str(station['parentId']).strip('_'),
        }

        await store_station(country, station_out, station_metadata)

    logger.info('%d stations stored', len(stations))

    removed = await validate_stations(country, set(stations.keys()))

    logger.info('%d obsolete stations removed', removed)
