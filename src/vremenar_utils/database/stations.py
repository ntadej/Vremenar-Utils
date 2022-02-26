"""Stations database helpers."""
from typing import Any, Union

from ..cli.common import CountryID
from .redis import redis


async def store_station(
    country: CountryID, station: dict[str, Any], metadata: dict[str, Any]
) -> None:
    """Store a station to redis."""
    id = station['id']

    async with redis.pipeline() as pipe:
        pipe.hmset(f'station:{country.value}:{id}', station)
        pipe.hmset(f'station:{country.value}:{id}', metadata)
        await pipe.execute()


async def validate_stations(country: CountryID, ids: set[str]) -> int:
    """Validate station IDs and remove obsolete."""
    ids_to_remove: set[str] = set()

    async with redis.client() as conn:
        cur = b'0'  # set initial cursor to 0
        while cur:
            cur, keys = await conn.scan(
                cur,  # type: ignore
                match=f'station:{country.value}:*',
            )
            for key in keys:
                id = await conn.hget(key, 'id')
                if id not in ids:
                    ids_to_remove.add(id)

    async with redis.client() as conn:
        for id in ids_to_remove:
            conn.delete(f'station:{country.value}:{id}')

    return len(ids_to_remove)


async def load_stations(
    country: CountryID,
) -> dict[str, dict[str, Union[str, int, float]]]:
    """Load stations from redis."""
    stations: dict[str, dict[str, Union[str, int, float]]] = {}
    async with redis.client() as conn:
        cur = b'0'  # set initial cursor to 0
        while cur:
            cur, keys = await conn.scan(
                cur,  # type: ignore
                match=f'station:{country.value}:*',
            )
            for key in keys:
                _, station = await conn.hscan(key, count=25)
                stations[station['id']] = station
    return stations
