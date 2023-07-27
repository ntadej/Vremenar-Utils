"""ARSO weather conditions utils."""
from datetime import datetime
from typing import Any

from httpx import AsyncClient

from vremenar_utils.cli.common import CountryID
from vremenar_utils.cli.logging import Logger
from vremenar_utils.database.redis import BatchedRedis, redis
from vremenar_utils.database.stations import load_stations

from . import BASEURL
from .database import BatchedWeather
from .stations import load_stations as load_local_stations


def weather_data_url(data_id: str) -> str:
    """Generate forecast map URL."""
    if data_id == "current":
        return f"{BASEURL}/uploads/probase/www/fproduct/json/sl/nowcast_si_latest.json"

    if data_id[0] == "d":
        return (
            f"{BASEURL}/uploads/probase/www/fproduct/json/sl/forecast_si_{data_id}.json"
        )

    return ""


def wind_direction_to_degrees(wind_direction: str) -> float:
    """Convert wind direction to degrees."""
    return {
        "S": 0.0,
        "SV": 45.0,
        "V": 90,
        "JV": 135.0,
        "J": 180.0,
        "JZ": 225.0,
        "Z": 270.0,
        "SZ": 315.0,
    }.get(wind_direction, -1)


def parse_feature(
    station_ids: list[str],
    feature: dict[str, Any],
) -> dict[str, Any] | None:
    """Parse ARSO feature."""
    if "properties" not in feature:
        return None

    properties = feature["properties"]

    station_id = properties["id"].strip("_")
    if station_id not in station_ids:
        return None

    timeline = properties["days"][0]["timeline"][0]
    time = datetime.strptime(timeline["valid"], "%Y-%m-%dT%H:%M:%S%z")
    icon = timeline["clouds_icon_wwsyn_icon"]

    if "txsyn" in timeline:
        temperature: float = float(timeline["txsyn"])
        temperature_low: float | None = float(timeline["tnsyn"])
    else:
        temperature = float(timeline["t"])
        temperature_low = None
    humidity: float | None = float(timeline["rh"]) if timeline["rh"] else None
    pressure_msl: float | None = float(timeline["msl"]) if timeline["msl"] else None
    wind_speed: float | None = float(timeline["ff_val"]) if timeline["ff_val"] else None
    wind_direction: float | None = wind_direction_to_degrees(timeline["dd_shortText"])

    return {
        "station_id": station_id,
        "timestamp": f"{int(time.timestamp())}000",
        "icon": icon,
        "wind_direction": wind_direction,
        "wind_speed": wind_speed,
        "temperature": temperature,
        "temperature_low": temperature_low,
        "humidity": humidity,
        "pressure_msl": pressure_msl,
    }


async def get_weather_data(
    logger: Logger,
    batch: BatchedRedis,
    station_ids: list[str],
    data_id: str,
) -> None:
    """Get weather conditions data from ID."""
    url: str = weather_data_url(data_id)

    logger.debug("ARSO URL: %s", url)

    async with AsyncClient() as client:
        response = await client.get(url)

    if response.status_code == 404:  # pragma: no cover
        return

    response_body = response.json()
    if "features" not in response_body:  # pragma: no cover
        return

    for feature in response_body["features"]:
        feature_data = parse_feature(station_ids, feature)
        if not feature_data:
            continue

        record = {"source": f"ARSO:{data_id}:{feature_data['station_id']}"}
        record.update(feature_data)

        await batch.add(record)


async def process_weather_data(
    logger: Logger,
    local_stations: bool | None = False,
) -> str:
    """Cache ARSO weather condition data."""
    # load stations to use
    station_ids: list[str] = []
    if local_stations:
        logger.info("Loading ARSO station IDs from the local database")
        station_ids = list(load_local_stations().keys())
    else:
        stations_dict = await load_stations(CountryID.Slovenia)
        station_ids = list(stations_dict.keys())

    async with redis.client() as db, BatchedWeather(db) as batch:
        for data_id in [
            "current",
            "d1h00",
            "d1h06",
            "d1h12",
            "d1h18",
            "d2h00",
            "d2h06",
            "d2h12",
            "d2h18",
            "d3h00",
            "d3h06",
            "d3h12",
            "d3h18",
            "d4h00",
            "d4",
            "d5",
            "d6",
            "d7",
            "d8",
            "d9",
            "d10",
        ]:
            await get_weather_data(logger, batch, station_ids, data_id)

    return "Processed all data"
