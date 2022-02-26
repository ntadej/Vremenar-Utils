"""DWD database utilities."""
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..cli.common import CountryID
from ..cli.logging import Logger
from ..database.redis import redis, Redis
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


async def store_mosmix_record(
    id: str, record: dict[str, str], connection: Optional[Redis] = None
) -> None:
    """Store MOSMIX records."""
    if connection is None:
        connection = redis

    async with connection.pipeline() as pipe:
        now = datetime.now(tz=timezone.utc)
        now = now.replace(minute=0, second=0, microsecond=0)
        reference = now + timedelta(hours=-2)
        record_time = datetime.fromtimestamp(
            float(record['timestamp'][:-3]), tz=timezone.utc
        )
        delta = record_time - reference

        # cleanup
        empty_keys = set()
        for key, value in record.items():
            if value is None:
                empty_keys.add(key)  # type: ignore
        for key in empty_keys:
            del record[key]

        # store in the DB
        key = f'mosmix:{id}'
        pipe.hset(key, mapping=record)
        pipe.expire(key, delta)
        await pipe.execute()
