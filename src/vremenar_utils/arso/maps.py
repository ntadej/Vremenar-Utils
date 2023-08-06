"""ARSO weather maps utils."""
from datetime import datetime, timedelta, timezone
from enum import Enum

from httpx import AsyncClient

from vremenar_utils.cli.logging import Logger, progress_bar
from vremenar_utils.database.redis import BatchedRedis, redis

from . import TIMEOUT, UPLOADS_BASEURL
from .database import BatchedMaps


class ObservationType(str, Enum):
    """Observation type enum."""

    Historical = "historical"
    Recent = "recent"
    Forecast = "forecast"


class MapType(str, Enum):
    """Map type enum."""

    Precipitation = "precipitation"
    CloudCoverage = "cloud"
    WindSpeed = "wind"
    Temperature = "temperature"
    HailProbability = "hail"


def weather_map_prefix(map_type: MapType) -> str:
    """Generate map URL prefix for type."""
    prefix = f"{UPLOADS_BASEURL}/nowcast/inca/inca"
    if map_type is MapType.Precipitation:
        return f"{prefix}_si0zm_"
    if map_type is MapType.CloudCoverage:
        return f"{prefix}_sp_"
    if map_type is MapType.WindSpeed:
        return f"{prefix}_wind_"
    if map_type is MapType.Temperature:
        return f"{prefix}_t2m_"
    if map_type is MapType.HailProbability:
        return f"{prefix}_hp_"
    raise RuntimeError()  # pragma: no cover


def weather_map_interval(map_type: MapType) -> int:
    """Get map interval for type."""
    if map_type in [MapType.Precipitation, MapType.HailProbability]:
        return 5
    if map_type is MapType.CloudCoverage:
        return 30
    if map_type in [MapType.WindSpeed, MapType.Temperature]:
        return 60
    raise RuntimeError()  # pragma: no cover


def weather_map_expiration(map_type: MapType) -> int:
    """Get map expiration for type."""
    if map_type in [MapType.Precipitation, MapType.HailProbability]:
        return 3
    if map_type is MapType.CloudCoverage:
        return 6
    if map_type in [MapType.WindSpeed, MapType.Temperature]:
        return 6
    raise RuntimeError()  # pragma: no cover


def weather_map_forecast(map_type: MapType) -> list[tuple[int, str]]:
    """Get map forecast list for type."""
    if map_type in [MapType.Precipitation, MapType.HailProbability]:
        return []
    if map_type is MapType.CloudCoverage:
        return [(30, "0030"), (60, "0100")]
    if map_type in [MapType.WindSpeed, MapType.Temperature]:
        return [
            (60, "0100"),
            (120, "0200"),
            (180, "0300"),
            (240, "0400"),
            (300, "0500"),
            (360, "0600"),
        ]
    raise RuntimeError()  # pragma: no cover


async def get_map_data(
    logger: Logger,
    batch: BatchedRedis,
    map_type: MapType,
) -> None:
    """Get weather map data for type."""
    url_prefix: str = weather_map_prefix(map_type)
    interval: int = weather_map_interval(map_type)
    expiration: int = weather_map_expiration(map_type)

    logger.info("ARSO URL prefix for %s: %s", map_type, url_prefix)

    async with AsyncClient() as client:
        now = datetime.now(tz=timezone.utc)
        now = now.replace(
            minute=now.minute - (now.minute % interval),
            second=0,
            microsecond=0,
        )

        while True:
            url = f"{url_prefix}{now:%Y%m%d-%H%M+0000}.png"
            logger.info("Test URL: %s", url)

            response = await client.head(url, timeout=TIMEOUT)
            if response.status_code == 404:  # pragma: no cover
                now -= timedelta(minutes=interval)
                continue

            logger.info("Found!")
            break

    for i in range(0, int(expiration / interval * 60 + 1)):
        time = now - timedelta(minutes=i * interval)
        url = f"{url_prefix}{time:%Y%m%d-%H%M+0000}.png"

        logger.debug("Output URL: %s", url)

        record = {
            "type": map_type.value,
            "expiration": expiration,
            "timestamp": f"{int(time.timestamp())}000",
            "url": url,
            "observation": ObservationType.Recent.value
            if i == 0
            else ObservationType.Historical.value,
        }

        await batch.add(record)

    # Forecast
    for delta, delta_str in weather_map_forecast(map_type):
        time = now + timedelta(minutes=delta)
        url = f"{url_prefix}{now:%Y%m%d-%H%M}+{delta_str}.png"

        logger.debug("Output URL: %s", url)

        record = {
            "type": map_type.value,
            "expiration": expiration,
            "timestamp": f"{int(time.timestamp())}000",
            "url": url,
            "observation": ObservationType.Forecast.value,
        }

        await batch.add(record)


async def process_map_data(logger: Logger) -> None:
    """Cache ARSO weather maps data."""
    async with redis.client() as db, BatchedMaps(db) as batch:  # pragma: no branch
        # TODO: figure out why this is not covered
        with progress_bar(transient=True) as progress:  # pragma: no cover
            task = progress.add_task("Processing", total=len(MapType))
            for map_type in MapType:
                await get_map_data(logger, batch, map_type)
                progress.update(task, advance=1)

    logger.info("Processed all data")
