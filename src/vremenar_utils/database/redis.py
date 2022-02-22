"""Redis utilities."""
import aioredis

redis: aioredis.Redis = aioredis.from_url(  # type: ignore
    'redis://localhost', decode_responses=True
)
