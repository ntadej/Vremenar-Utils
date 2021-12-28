"""Vremenar database utilities."""
from deta.base import _Base  # type: ignore
from typing import Any, Optional


class BatchedPut:
    """Put items to DB in batches."""

    def __init__(self, db: _Base) -> None:
        """Initialise with DB."""
        self.db = db
        self.queue: list[dict[str, Any]] = []
        self.limit = 25

    def __enter__(self) -> 'BatchedPut':
        """Context manager init."""
        return self

    def __exit__(self, type, value, traceback):
        """Context manager exit."""
        self._drain()

    def put(self, item: dict[str, Any], key: Optional[str] = None) -> None:
        """Put item to the DB (add it in the queue)."""
        if len(self.queue) == self.limit:
            self._drain()

        if key:
            item['key'] = key

        self.queue.append(item)

    def _drain(self) -> None:
        """Drain the queue."""
        if self.db and self.queue:
            self.db.put_many(self.queue)
        self.queue.clear()
