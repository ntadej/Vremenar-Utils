"""Stations database helpers."""
from typing import Any, Optional, Union

from ..cli.common import CountryID
from .redis import redis


async def store_station(
    country: CountryID,
    station: dict[str, Any],
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    """Store a station to redis."""
    station_id = station['id']

    async with redis.pipeline() as pipeline:
        pipeline.sadd(f'station:{country.value}', station_id)
        pipeline.hset(f'station:{country.value}:{station_id}', mapping=station)
        if metadata is not None:
            pipeline.hset(f'station:{country.value}:{station_id}', mapping=metadata)
        await pipeline.execute()


async def validate_stations(country: CountryID, ids: set[str]) -> int:
    """Validate station IDs and remove obsolete."""
    existing_ids: set[str] = await redis.smembers(f'station:{country.value}')
    ids_to_remove: set[str] = set()

    for id in existing_ids:
        if id not in ids:
            ids_to_remove.add(id)

    async with redis.client() as connection:
        for id in ids_to_remove:
            async with connection.pipeline() as pipeline:
                pipeline.srem(f'station:{country.value}', id)
                pipeline.delete(f'station:{country.value}:{id}')
                await pipeline.execute()

    return len(ids_to_remove)


async def load_stations(
    country: CountryID,
) -> dict[str, dict[str, Union[str, int, float]]]:
    """Load stations from redis."""
    stations: dict[str, dict[str, Union[str, int, float]]] = {}
    async with redis.client() as connection:
        ids: set[str] = await redis.smembers(f'station:{country.value}')
        async with connection.pipeline(transaction=False) as pipeline:
            for id in ids:
                pipeline.hgetall(f'station:{country.value}:{id}')
            response = await pipeline.execute()

    for station in response:
        stations[station['id']] = station

    return stations
