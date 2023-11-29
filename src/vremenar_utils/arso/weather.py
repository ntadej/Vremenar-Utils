"""ARSO weather conditions utils."""
from datetime import datetime
from typing import Any

from httpx import AsyncClient, codes

from vremenar_utils.cli.common import CountryID
from vremenar_utils.cli.logging import Logger, progress_bar
from vremenar_utils.database.redis import BatchedRedis, redis
from vremenar_utils.database.stations import load_stations

from . import BASEURL, TIMEOUT
from .database import BatchedMaps, BatchedWeather
from .maps import ObservationType
from .stations import load_stations as load_local_stations

data_ids = [
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
]


def weather_data_url(data_id: str) -> str:
    """Generate forecast map URL."""
    if data_id == "current":
        return f"{BASEURL}/uploads/probase/www/fproduct/json/sl/nowcast_si_latest.json"

    return f"{BASEURL}/uploads/probase/www/fproduct/json/sl/forecast_si_{data_id}.json"


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
    if "properties" not in feature:  # pragma: no cover
        return None

    properties = feature["properties"]

    station_id = properties["id"].strip("_")
    if station_id not in station_ids:  # pragma: no cover
        return None

    timeline = properties["days"][0]["timeline"][0]
    time = datetime.strptime(timeline["valid"], "%Y-%m-%dT%H:%M:%S%z")
    icon = timeline["clouds_icon_wwsyn_icon"]

    if "txsyn" in timeline:
        temperature: float = float(timeline["txsyn"])
        temperature_low: float | None = float(timeline["tnsyn"])
    else:
        if timeline["t"] == "":  # pragma: no cover
            return None
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
    batch_maps: BatchedRedis,
    station_ids: list[str],
    data_id: str,
) -> None:
    """Get weather conditions data from ID."""
    url: str = weather_data_url(data_id)

    logger.info("ARSO URL for %s: %s", data_id, url)

    async with AsyncClient() as client:
        response = await client.get(url, timeout=TIMEOUT)

    if response.status_code != codes.OK:  # pragma: no cover
        return

    response_body = response.json()
    if "features" not in response_body:  # pragma: no cover
        return

    timestamp = None
    for feature in response_body["features"]:
        feature_data = parse_feature(station_ids, feature)
        if not feature_data:  # pragma: no cover
            continue

        record = {"source": f"ARSO:{data_id}:{feature_data['station_id']}"}
        record.update(feature_data)
        timestamp = feature_data["timestamp"]

        await batch.add(record)

    await batch_maps.add(
        {
            "type": "condition",
            "expiration": 0,
            "timestamp": timestamp,
            "url": f"/stations/map/current?country={CountryID.Slovenia.value}"
            if data_id == "current"
            else f"/stations/map/{timestamp}?country={CountryID.Slovenia.value}",
            "observation": ObservationType.Recent.value
            if data_id == "current"
            else ObservationType.Forecast.value,
        },
    )


async def process_weather_data(
    logger: Logger,
    local_stations: bool | None = False,
) -> None:
    """Cache ARSO weather condition data."""
    # load stations to use
    station_ids: list[str] = []
    if local_stations:
        logger.info("Loading ARSO station IDs from the local database")
        station_ids = list(load_local_stations().keys())
    else:
        stations_dict = await load_stations(CountryID.Slovenia)
        station_ids = list(stations_dict.keys())

    async with redis.client() as db, BatchedWeather(  # pragma: no branch
        db,
    ) as batch, BatchedMaps(
        db,
    ) as batch_maps:
        # TODO: figure out why this is not covered
        with progress_bar(transient=True) as progress:  # pragma: no cover
            task = progress.add_task("Processing", total=len(data_ids))
            for data_id in data_ids:
                await get_weather_data(logger, batch, batch_maps, station_ids, data_id)
                progress.update(task, advance=1)

    logger.info("Processed all data")
