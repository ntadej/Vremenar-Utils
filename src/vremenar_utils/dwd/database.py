"""DWD database utilities."""
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
    country = CountryID.Germany
    stations = load_stations()

    for station_id, station in stations.items():
        station_out = {
            "id": station_id,
            "name": station["name"],
            "latitude": station["lat"],
            "longitude": station["lon"],
            "altitude": station["altitude"],
            "zoom_level": zoom_level_conversion(
                str(station["type"]),
                float(station["admin"]),
            ),
            "forecast_only": int(not station["has_reports"]),
        }

        station_metadata = {
            "status": station["status"],
            "DWD_ID": station["dwd_station_id"],
        }

        await store_station(country, station_out, station_metadata)

    logger.info("%d stations stored", len(stations))

    removed = await validate_stations(country, set(stations.keys()))

    logger.info("%d obsolete stations removed", removed)


class BatchedMosmix(BatchedRedis):
    """Batched MOSMIX save."""

    def process(
        self,
        pipeline: RedisPipeline[str],
        record: dict[str, str | int | float | None],
    ) -> None:
        """Process MOSMIX record."""
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
        set_key = f"mosmix:{record['timestamp']}"
        key = f"mosmix:{record['timestamp']}:{record['station_id']}"
        pipeline.sadd(set_key, key)
        pipeline.expire(set_key, delta)
        pipeline.hset(
            key,
            mapping=cast(Mapping[bytes | str, bytes | float | int | str], record),
        )
        pipeline.expire(key, delta)


class BatchedCurrentWeather(BatchedRedis):
    """Batched current weather save."""

    def process(
        self,
        pipeline: RedisPipeline[str],
        record: dict[str, str | int | float | None],
    ) -> None:
        """Process current weather record."""
        # cleanup
        empty_keys = set()
        for key, value in record.items():
            if value is None:
                empty_keys.add(key)
        for key in empty_keys:
            record[key] = ""

        # store in the DB
        key = f"dwd:current:{record['station_id']}"
        pipeline.hset(
            key,
            mapping=cast(Mapping[bytes | str, bytes | float | int | str], record),
        )
        pipeline.expire(key, timedelta(hours=3))
