"""ARSO database utilities."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from vremenar_utils.cli.common import CountryID
from vremenar_utils.database.redis import BatchedRedis, RedisPipeline
from vremenar_utils.database.stations import store_station, validate_stations

from .stations import load_stations, zoom_level_conversion

if TYPE_CHECKING:  # pragma: no cover
    from vremenar_utils.cli.logging import Logger


async def store_stations(logger: Logger) -> None:
    """Store DWD stations to redis."""
    country = CountryID.Slovenia
    stations = load_stations()

    for station_id, station in stations.items():
        station_out = {
            "id": station_id,
            "name": station["title"],
            "latitude": station["latitude"],
            "longitude": station["longitude"],
            "altitude": station["altitude"],
            "zoom_level": zoom_level_conversion(float(station["zoomLevel"])),
            "forecast_only": 0,
        }

        station_metadata = {
            "country": station["country"],
            "region": str(station["parentId"]).strip("_"),
        }

        await store_station(country, station_out, station_metadata)

    logger.info("%d stations stored", len(stations))

    removed = await validate_stations(country, set(stations.keys()))

    logger.info("%d obsolete stations removed", removed)


class BatchedWeather(BatchedRedis):
    """Batched ARSO weather information save."""

    def process(
        self: BatchedWeather,
        pipeline: RedisPipeline[str],
        record: dict[str, str | int | float | None],
    ) -> None:
        """Process ARSO weather records."""
        if not isinstance(record["timestamp"], str):  # pragma: no cover
            err = "Invalid 'timestamp' value"
            raise TypeError(err)

        now = datetime.now(tz=UTC)
        now = now.replace(minute=0, second=0, microsecond=0)
        reference = now + timedelta(hours=-2)
        record_time = datetime.fromtimestamp(
            float(record["timestamp"][:-3]),
            tz=UTC,
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
        sub_key = record["timestamp"]
        if isinstance(record["source"], str) and "current" in record["source"]:
            sub_key = "current"
        set_key = f"arso:weather:{sub_key}"
        key = f"arso:weather:{sub_key}:{record['station_id']}"
        pipeline.sadd(set_key, key)
        pipeline.expire(set_key, delta)
        pipeline.hset(
            key,
            mapping=cast(Mapping[bytes | str, bytes | float | int | str], record),
        )
        pipeline.expire(key, delta)


class BatchedMaps(BatchedRedis):
    """Batched ARSO weather map save."""

    def process(
        self: BatchedMaps,
        pipeline: RedisPipeline[str],
        record: dict[str, str],
    ) -> None:
        """Process ARSO weather map images."""
        expiration = int(record["expiration"])
        sub_key = record["timestamp"]
        if isinstance(record["url"], str) and "current" in record["url"]:
            expiration = 2
            sub_key = "current"

        now = datetime.now(tz=UTC)
        now = now.replace(minute=0, second=0, microsecond=0)
        reference = now + timedelta(hours=-expiration)
        record_time = datetime.fromtimestamp(
            float(record["timestamp"][:-3]),
            tz=UTC,
        )
        delta = record_time - reference

        key = f"arso:map:{record['type']}:{sub_key}"
        # clean helper vairables
        del record["type"]
        del record["expiration"]
        # store in the DB
        pipeline.hset(
            key,
            mapping=cast(Mapping[bytes | str, bytes | float | int | str], record),
        )
        pipeline.expire(key, delta)
