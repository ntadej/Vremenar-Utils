"""MeteoAlarm database utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from vremenar_utils.cli.common import CountryID, LanguageID
from vremenar_utils.database.redis import BatchedRedis, Redis, RedisPipeline, redis

from .common import AlertArea, AlertInfo, AlertNotificationInfo

if TYPE_CHECKING:
    from collections.abc import Mapping


async def get_alert_ids(country: CountryID) -> set[str]:
    """Get alert IDs from redis."""
    existing_alerts: set[str] = await redis.smembers(f"alert:{country}")
    return existing_alerts


async def get_alert_info(
    country: CountryID,
    alert_id: str,
) -> dict[str, dict[str, str]]:
    """Get alert info from ID."""
    alert = {}
    async with (
        redis.client() as connection,
        connection.pipeline(
            transaction=False,
        ) as pipeline,
    ):
        pipeline.hgetall(f"alert:{country}:{alert_id}:info")
        pipeline.smembers(f"alert:{country}:{alert_id}:areas")
        for language in LanguageID:
            pipeline.hgetall(f"alert:{country}:{alert_id}:localised_{language}")
        pipeline.hgetall(f"alert:{country}:{alert_id}:notifications")
        response = await pipeline.execute()
    alert["info"] = response[0]
    alert["areas"] = response[1]
    for i, language in enumerate(LanguageID):
        alert[language] = response[i + 2]
    alert["notifications"] = response[-1]
    return alert


async def store_alert(country: CountryID, alert: AlertInfo) -> None:
    """Store alert to redis."""
    async with redis.pipeline() as pipeline:
        pipeline.sadd(f"alert:{country}", alert.id)
        pipeline.hset(
            f"alert:{country}:{alert.id}:info",
            mapping=cast(
                "Mapping[bytes | str, bytes | float | int | str]",
                alert.to_info_dict(),
            ),
        )
        pipeline.sadd(f"alert:{country}:{alert.id}:areas", *alert.areas)
        for language in LanguageID:
            pipeline.hset(
                f"alert:{country}:{alert.id}:localised_{language}",
                mapping=cast(
                    "Mapping[bytes | str, bytes | float | int | str]",
                    alert.to_localised_dict(language),
                ),
            )
        pipeline.hset(
            f"alert:{country}:{alert.id}:notifications",
            mapping=cast(
                "Mapping[bytes | str, bytes | float | int | str]",
                AlertNotificationInfo(alert.id).to_dict(),
            ),
        )
        await pipeline.execute()


async def delete_alert(country: CountryID, alert_id: str) -> None:
    """Delete alert from redis."""
    async with redis.pipeline() as pipeline:
        pipeline.srem(f"alert:{country}", alert_id)
        pipeline.delete(
            f"alert:{country}:{alert_id}:info",
            f"alert:{country}:{alert_id}:areas",
            f"alert:{country}:{alert_id}:notifications",
        )
        pipeline.delete(
            *[
                f"alert:{country}:{alert_id}:localised_{language}"
                for language in LanguageID
            ],
        )
        await pipeline.execute()


async def get_alert_area_map(country: CountryID) -> dict[str, set[str]]:
    """Get alert-area map from redis."""
    alert_areas: dict[str, set[str]] = {}
    alert_ids: set[str] = await get_alert_ids(country)

    async with redis.client() as connection:
        for alert_id in alert_ids:
            alert_areas[alert_id] = await connection.smembers(
                f"alert:{country}:{alert_id}:areas",
            )
    return alert_areas


async def store_alerts_areas(country: CountryID, areas: list[AlertArea]) -> None:
    """Store alerts areas to redis."""
    async with redis.client() as connection:
        existing_areas: set[str] = await connection.smembers(
            f"alerts_area:{country}",
        )
        area_codes: set[str] = set()
        for area in areas:
            async with connection.pipeline() as pipeline:
                area_codes.add(area.code)
                pipeline.hset(
                    f"alerts_area:{country}:{area.code}:info",
                    mapping=cast(
                        "Mapping[bytes | str, bytes | float | int | str]",
                        area.to_dict_for_database(),
                    ),
                )
                pipeline.sadd(f"alerts_area:{country}", area.code)
                await pipeline.execute()

        # validate
        for code in existing_areas:  # pragma: no cover
            if code not in area_codes:
                async with connection.pipeline() as pipeline:
                    pipeline.srem(f"alerts_area:{country}", code)
                    pipeline.delete(f"alerts_area:{country}:{code}:info")
                    pipeline.delete(f"alerts_area:{country}:{code}:alerts")
                    await pipeline.execute()


async def store_alerts_for_area(
    country: CountryID,
    area: str,
    alerts: set[str],
) -> None:
    """Store alert IDs for area to redis."""
    async with redis.pipeline() as pipeline:
        pipeline.delete(f"alerts_area:{country}:{area}:alerts")
        if alerts:
            pipeline.sadd(f"alerts_area:{country}:{area}:alerts", *alerts)
        await pipeline.execute()


class BatchedNotifyAnnounce(BatchedRedis):
    """Batched alert announcement notifications."""

    def __init__(self, connection: Redis[str], country: CountryID) -> None:
        """Initialise batched notify for a country."""
        self.country = country
        super().__init__(connection)

    def process(self, pipeline: RedisPipeline[str], alert_id: str) -> None:
        """Process alert on announcement nofitication."""
        key = f"alert:{self.country}:{alert_id}:notifications"
        pipeline.hset(key, mapping={"announce": 1})


class BatchedNotifyOnset(BatchedRedis):
    """Batched alert onset notifications."""

    def __init__(self, connection: Redis[str], country: CountryID) -> None:
        """Initialise batched notify for a country."""
        self.country = country
        super().__init__(connection)

    def process(self, pipeline: RedisPipeline[str], alert_id: str) -> None:
        """Process alert on onset nofitication."""
        key = f"alert:{self.country}:{alert_id}:notifications"
        pipeline.hset(key, mapping={"onset": 1})
