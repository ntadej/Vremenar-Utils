"""Redis utilities."""
from aioredis import Redis, from_url
from os import getenv

from ..cli.logging import Logger

db_env: str = getenv('VREMENAR_DATABASE', 'staging')
database: int = {
    'staging': 0,
    'production': 1,
    'test': 2,
}.get(db_env, 0)

redis: Redis = from_url(  # type: ignore
    f'redis://localhost/{database}', decode_responses=True
)


def database_info(logger: Logger) -> None:
    """Log the database info."""
    logger.debug('Using %s database with ID %d', db_env, database)


__all__ = ['redis', 'Redis']
