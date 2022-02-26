"""MeteoAlarm database utilities."""
from ..cli.common import CountryID, LanguageID
from ..database.redis import redis

from .common import AlertInfo, AlertNotificationInfo


async def get_alert_ids(country: CountryID) -> set[str]:
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


async def store_alert(country: CountryID, alert: AlertInfo) -> None:
    """Store alert to redis."""
    async with redis.pipeline() as pipe:
        pipe.hset(
            f'alert:{country.value}:{alert.id}:info', mapping=alert.to_info_dict()
        )
        pipe.sadd(f'alert:{country.value}:{alert.id}:areas', *alert.areas)
        for language in LanguageID:
            pipe.hset(
                f'alert:{country.value}:{alert.id}:localised_{language.value}',
                mapping=alert.to_localised_dict(language),
            )
        pipe.hset(
            f'alert:{country.value}:{alert.id}:notifications',
            mapping=AlertNotificationInfo(alert.id).to_dict(),
        )
        await pipe.execute()


async def delete_alert(country: CountryID, id: str) -> None:
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
                for language in LanguageID
            ]
        )
        await pipe.execute()


async def get_alert_area_map(country: CountryID) -> dict[str, set[str]]:
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
    country: CountryID, area: str, alerts: set[str]
) -> None:
    """Store alert IDs for area to redis."""
    async with redis.pipeline() as pipe:
        pipe.delete(f'alerts_area:{country.value}:{area}:alerts')
        if alerts:
            pipe.sadd(f'alerts_area:{country.value}:{area}:alerts', *alerts)
        await pipe.execute()
