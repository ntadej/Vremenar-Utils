"""Redis utilities."""
from os import getenv
from typing import Any

from redis.asyncio import Redis, from_url
from redis.asyncio.client import Pipeline as RedisPipeline

from vremenar_utils.cli.logging import Logger

db_env: str = getenv("VREMENAR_DATABASE", "staging")
database: int = {
    "staging": 0,
    "production": 1,
    "test": 2,
}.get(db_env, 0)

redis: "Redis[str]" = from_url(f"redis://localhost/{database}", decode_responses=True)


def database_info(logger: Logger) -> None:
    """Log the database info."""
    logger.debug("Using %s database with ID %d", db_env, database)


class BatchedRedis:
    """Put items to redis in batches."""

    def __init__(self, connection: "Redis[str]", limit: int | None = 1000) -> None:
        """Initialise with DB."""
        self.connection = connection
        self.queue: list[Any] = []
        self.limit = limit

    async def __aenter__(self) -> "BatchedRedis":
        """Context manager init."""
        return self

    async def __aexit__(self, *args: Any) -> None:  # noqa: ANN401
        """Context manager exit."""
        await self._drain()

    async def add(self, item: Any) -> None:  # noqa: ANN401
        """Put item to the DB (add it in the queue)."""
        if len(self.queue) == self.limit:
            await self._drain()

        self.queue.append(item)

    def process(
        self,
        pipeline: "RedisPipeline[str]",
        item: Any,  # noqa: ANN401
    ) -> None:
        """Process items in queue."""
        err = "BatchedRedis needs to be subclassed and process implemented"
        raise NotImplementedError(err)

    async def _drain(self) -> None:
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

    def process(self, pipeline: "RedisPipeline[str]", item: str) -> None:
        """Process items in queue."""
        pipeline.delete(item)


__all__ = ["redis", "Redis", "RedisPipeline", "BatchedRedis", "BatchedRedisDelete"]
