"""Redis utilities."""
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from redis.asyncio import Redis, from_url
from redis.asyncio.client import Pipeline as RedisPipeline

if TYPE_CHECKING:
    from vremenar_utils.cli.config import Configuration
    from vremenar_utils.cli.logging import Logger

redis: Redis[str]


class DatabaseType(str, Enum):
    Staging = "staging"
    Production = "production"
    Test = "test"


def init_database(logger: Logger, config: Configuration) -> None:
    """Initialize database."""
    global redis  # noqa: PLW0603

    database: int = {
        DatabaseType.Staging: 0,
        DatabaseType.Production: 1,
        DatabaseType.Test: 2,
    }.get(config.database_type, 0)

    logger.info("Using %s database with ID %d", config.database_type.value, database)

    redis = from_url(f"redis://localhost/{database}", decode_responses=True)


class BatchedRedis:
    """Put items to redis in batches."""

    def __init__(
        self: BatchedRedis,
        connection: Redis[str],
        limit: int | None = 1000,
    ) -> None:
        """Initialise with DB."""
        self.connection = connection
        self.queue: list[Any] = []
        self.limit = limit

    async def __aenter__(self: BatchedRedis) -> BatchedRedis:
        """Context manager init."""
        return self

    async def __aexit__(self: BatchedRedis, *args: Any) -> None:  # noqa: ANN401
        """Context manager exit."""
        await self._drain()

    async def add(self: BatchedRedis, item: Any) -> None:  # noqa: ANN401
        """Put item to the DB (add it in the queue)."""
        if len(self.queue) == self.limit:
            await self._drain()

        self.queue.append(item)

    def process(
        self: BatchedRedis,
        pipeline: RedisPipeline[str],  # noqa: ARG002
        item: Any,  # noqa: ANN401, ARG002
    ) -> None:
        """Process items in queue."""
        err = "BatchedRedis needs to be subclassed and process implemented"
        raise NotImplementedError(err)

    async def _drain(self: BatchedRedis) -> None:
        """Drain the queue."""
        if not self.connection:  # pragma: no cover
            err = "Invalid redis connection"
            raise RuntimeError(err)

        if not self.queue:  # pragma: no cover
            # empty queue
            return

        async with self.connection.pipeline() as pipeline:
            for item in self.queue:
                self.process(pipeline, item)
            await pipeline.execute()

        self.queue.clear()


class BatchedRedisDelete(BatchedRedis):
    """Batch delete redis keys."""

    def process(
        self: BatchedRedisDelete,
        pipeline: RedisPipeline[str],
        item: str,
    ) -> None:
        """Process items in queue."""
        pipeline.delete(item)


__all__ = ["redis", "Redis", "RedisPipeline", "BatchedRedis", "BatchedRedisDelete"]
