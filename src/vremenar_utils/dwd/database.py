"""DWD database utilities."""
from datetime import datetime, timedelta, timezone
from typing import Any

from ..cli.common import CountryID
from ..cli.logging import Logger
from ..database.redis import Redis, BatchedRedis
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


class BatchedMosmix(BatchedRedis):
    """Batched MOSMIX save."""

    def process(self, pipeline: Redis, record: dict[str, Any]) -> None:
        """Process MOSMIX record."""
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
                empty_keys.add(key)
        for key in empty_keys:
            del record[key]

        # store in the DB
        set_key = f"mosmix:{record['timestamp']}"
        key = f"mosmix:{record['timestamp']}:{record['station_id']}"
        pipeline.sadd(set_key, key)
        pipeline.expire(set_key, delta)
        pipeline.hset(key, mapping=record)
        pipeline.expire(key, delta)


class BatchedCurrentWeather(BatchedRedis):
    """Batched current weather save."""

    def process(self, pipeline: Redis, record: dict[str, Any]) -> None:
        """Process current weather record."""
        country = CountryID.Germany
        # cleanup
        empty_keys = set()
        for key, value in record.items():
            if value is None:
                empty_keys.add(key)
        for key in empty_keys:
            record[key] = ''

        # store in the DB
        key = f"current:{country.value}:{record['station_id']}"
        pipeline.hset(key, mapping=record)
        pipeline.expire(key, timedelta(hours=3))
