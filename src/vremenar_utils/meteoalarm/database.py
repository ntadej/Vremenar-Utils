"""MeteoAlarm database utilities."""
from ..database.redis import redis

from .common import AlertCountry, AlertInfo, AlertLanguage, AlertNotificationInfo


async def get_alert_ids(country: AlertCountry) -> set[str]:
    """Get alert IDs from redis."""
    existing_alerts: set[str] = set()

    async with redis.client() as conn:
        cur = b'0'  # set initial cursor to 0
        while cur:
            cur, keys = await conn.scan(
                cur,  # type: ignore
                match=f'alert:{country.value}:*:info',
            )
            for key in keys:
                existing_alerts.add(await conn.hget(key, 'id'))

    return existing_alerts


async def store_alert(country: AlertCountry, alert: AlertInfo) -> None:
    """Store alert to redis."""
    async with redis.pipeline() as pipe:
        pipe.hmset(f'alert:{country.value}:{alert.id}:info', alert.to_info_dict())
        pipe.sadd(f'alert:{country.value}:{alert.id}:areas', *alert.areas)
        for language in AlertLanguage:
            pipe.hmset(
                f'alert:{country.value}:{alert.id}:localised_{language.value}',
                alert.to_localised_dict(language),
            )
        pipe.hmset(
            f'alert:{country.value}:{alert.id}:notifications',
            AlertNotificationInfo(alert.id).to_dict(),
        )
        await pipe.execute()


async def delete_alert(country: AlertCountry, id: str) -> None:
    """Delete alert from redis."""
    async with redis.pipeline() as pipe:
        pipe.delete(
            f'alert:{country.value}:{id}:info',
            f'alert:{country.value}:{id}:areas',
            f'alert:{country.value}:{id}:notifications',
        )
        pipe.delete(
            *[
                f'alert:{country.value}:{id}:localised_{language.value}'
                for language in AlertLanguage
            ]
        )
        await pipe.execute()


async def get_alert_area_map(country: AlertCountry) -> dict[str, set[str]]:
    """Get alert-area map from redis."""
    alert_areas: dict[str, set[str]] = {}
    async with redis.client() as conn:
        cur = b'0'  # set initial cursor to 0
        while cur:
            cur, keys = await conn.scan(
                cur,  # type: ignore
                match=f'alert:{country.value}:*:info',
            )
            for key in keys:
                alert_areas[await conn.hget(key, 'id')] = await conn.smembers(
                    key.replace('info', 'areas')
                )
    return alert_areas


async def store_alerts_for_area(
    country: AlertCountry, area: str, alerts: set[str]
) -> None:
    """Store alert IDs for area to redis."""
    async with redis.pipeline() as pipe:
        pipe.delete(f'alerts_area:{country.value}:{area}:alerts')
        if alerts:
            pipe.sadd(f'alerts_area:{country.value}:{area}:alerts', *alerts)
        await pipe.execute()
