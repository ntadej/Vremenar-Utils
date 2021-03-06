"""Redis utilities."""
from aioredis import Redis, from_url
from os import getenv
from typing import Any, Optional

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


class BatchedRedis:
    """Put items to redis in batches."""

    def __init__(self, connection: Redis, limit: Optional[int] = 1000) -> None:
        """Initialise with DB."""
        self.connection = connection
        self.queue: list[Any] = []
        self.limit = limit

    async def __aenter__(self) -> 'BatchedRedis':
        """Context manager init."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Context manager exit."""
        await self._drain()

    async def add(self, item: Any) -> None:
        """Put item to the DB (add it in the queue)."""
        if len(self.queue) == self.limit:
            await self._drain()

        self.queue.append(item)

    def process(self, pipeline: Redis, item: Any) -> None:
        """Process items in queue."""
        raise NotImplementedError(
            'BatchedRedis needs to be subclassed and process implemented'
        )

    async def _drain(self) -> None:
        """Drain the queue."""
        if self.connection and self.queue:
            async with self.connection.pipeline() as pipeline:
                for item in self.queue:
                    self.process(pipeline, item)
                await pipeline.execute()
        self.queue.clear()


class BatchedRedisDelete(BatchedRedis):
    """Batch delete redis keys."""

    def process(self, pipeline: Redis, item: str) -> None:
        """Process items in queue."""
        pipeline.delete(item)


__all__ = ['redis', 'Redis', 'BatchedRedis', 'BatchedRedisDelete']
