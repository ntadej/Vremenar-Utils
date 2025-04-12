"""Stations database helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from .redis import redis

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vremenar_utils.cli.common import CountryID


async def store_station(
    country: CountryID,
    station: dict[str, str | int | float],
    metadata: dict[str, str | int | float] | None = None,
) -> None:
    """Store a station to redis."""
    station_id = station["id"]

    async with redis.pipeline() as pipeline:
        pipeline.sadd(f"station:{country.value}", station_id)
        if "latitude" in station and "longitude" in station:
            pipeline.geoadd(
                f"location:{country.value}",
                (station["longitude"], station["latitude"], station_id),
            )
        pipeline.hset(
            f"station:{country.value}:{station_id}",
            mapping=cast("Mapping[bytes | str, bytes | float | int | str]", station),
        )
        if metadata is not None:  # pragma: no branch
            pipeline.hset(
                f"station:{country.value}:{station_id}",
                mapping=cast(
                    "Mapping[bytes | str, bytes | float | int | str]",
                    metadata,
                ),
            )
        await pipeline.execute()


async def validate_stations(country: CountryID, station_ids: set[str]) -> int:
    """Validate station IDs and remove obsolete."""
    existing_ids: set[str] = await redis.smembers(f"station:{country.value}")
    ids_to_remove: set[str] = set()

    for station_id in existing_ids:  # pragma: no cover
        if station_id not in station_ids:
            ids_to_remove.add(station_id)

    if ids_to_remove:  # pragma: no cover
        async with redis.client() as connection:
            for station_id in ids_to_remove:
                async with connection.pipeline() as pipeline:
                    pipeline.srem(f"station:{country.value}", station_id)
                    pipeline.delete(f"station:{country.value}:{station_id}")
                    await pipeline.execute()

    return len(ids_to_remove)


async def load_stations(
    country: CountryID,
) -> dict[str, dict[str, str | int | float]]:
    """Load stations from redis."""
    stations: dict[str, dict[str, str | int | float]] = {}
    async with redis.client() as connection:
        station_ids: set[str] = await redis.smembers(f"station:{country.value}")
        async with connection.pipeline(transaction=False) as pipeline:
            for station_id in station_ids:
                pipeline.hgetall(f"station:{country.value}:{station_id}")
            response = await pipeline.execute()

    for station in response:
        stations[station["id"]] = station

    return stations
