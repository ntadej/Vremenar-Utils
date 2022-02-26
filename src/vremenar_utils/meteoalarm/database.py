"""MeteoAlarm database utilities."""
from ..cli.common import CountryID, LanguageID
from ..database.redis import redis

from .common import AlertInfo, AlertNotificationInfo


async def get_alert_ids(country: CountryID) -> set[str]:
    """Get alert IDs from redis."""
    existing_alerts: set[str] = await redis.smembers(f'alert:{country.value}')
    return existing_alerts


async def store_alert(country: CountryID, alert: AlertInfo) -> None:
    """Store alert to redis."""
    async with redis.pipeline() as pipeline:
        pipeline.sadd(f'alert:{country.value}', alert.id)
        pipeline.hset(
            f'alert:{country.value}:{alert.id}:info', mapping=alert.to_info_dict()
        )
        pipeline.sadd(f'alert:{country.value}:{alert.id}:areas', *alert.areas)
        for language in LanguageID:
            pipeline.hset(
                f'alert:{country.value}:{alert.id}:localised_{language.value}',
                mapping=alert.to_localised_dict(language),
            )
        pipeline.hset(
            f'alert:{country.value}:{alert.id}:notifications',
            mapping=AlertNotificationInfo(alert.id).to_dict(),
        )
        await pipeline.execute()


async def delete_alert(country: CountryID, id: str) -> None:
    """Delete alert from redis."""
    async with redis.pipeline() as pipeline:
        pipeline.srem(f'alert:{country.value}', id)
        pipeline.delete(
            f'alert:{country.value}:{id}:info',
            f'alert:{country.value}:{id}:areas',
            f'alert:{country.value}:{id}:notifications',
        )
        pipeline.delete(
            *[
                f'alert:{country.value}:{id}:localised_{language.value}'
                for language in LanguageID
            ]
        )
        await pipeline.execute()


async def get_alert_area_map(country: CountryID) -> dict[str, set[str]]:
    """Get alert-area map from redis."""
    alert_areas: dict[str, set[str]] = {}
    alert_ids: set[str] = await get_alert_ids(country)

    async with redis.client() as connection:
        for id in alert_ids:
            alert_areas[id] = await connection.smembers(
                f'alert:{country.value}:{id}:areas'
            )
    return alert_areas


async def store_alerts_for_area(
    country: CountryID, area: str, alerts: set[str]
) -> None:
    """Store alert IDs for area to redis."""
    async with redis.pipeline() as pipeline:
        pipeline.delete(f'alerts_area:{country.value}:{area}:alerts')
        if alerts:
            pipeline.sadd(f'alerts_area:{country.value}:{area}:alerts', *alerts)
        await pipeline.execute()
