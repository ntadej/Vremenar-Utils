"""OSM Nominatim Utils."""

from asyncio import sleep
from json import dump, load
from pathlib import Path

from httpx2 import AsyncClient

from vremenar_utils import __version__
from vremenar_utils.cli.logging import Logger
from vremenar_utils.dwd.stations import load_stations_from_csv

OSM_CACHE_DIR: Path = Path.cwd() / ".cache/osm"
OSM_URL = "https://nominatim.openstreetmap.org"

HEADERS = {
    "user-agent": f"Vremenar-Utils/{__version__}",
    "referer": "https://vremenar.app",
}


def create_osm_cache_dir() -> None:
    """Create OSM cache directory."""
    OSM_CACHE_DIR.mkdir(parents=True, exist_ok=True)


async def update_dwd_cache(
    logger: Logger,
    input_file: Path,
    *,
    test_mode: bool = False,
) -> None:
    """Update DWD Nominatim cache."""
    # Load stations
    stations: dict[str, dict[str, str | int | float]] = {}
    with input_file.open(encoding="utf-8") as csv_file:
        stations = load_stations_from_csv(csv_file)

    # Loop over stations
    for station_id, station in stations.items():  # pragma: no branch
        logger.info("Processing station: %s/%s", station_id, station["station_name"])

        lat = station["lat"]
        lon = station["lon"]

        reverse_cache = OSM_CACHE_DIR / f"DWD:{station_id}:reverse.json"
        details_cache = OSM_CACHE_DIR / f"DWD:{station_id}:reverse_details.json"

        if not reverse_cache.is_file():
            await sleep(1)

            reverse_url = (
                f"{OSM_URL}/reverse.php?lat={lat}&lon={lon}&zoom=16&format=jsonv2"
            )
            logger.debug("Reverse URL: %s", reverse_url)
            async with AsyncClient() as client:
                response = await client.get(reverse_url, headers=HEADERS)
            reverse_result = response.json()

            with reverse_cache.open("w") as file:
                dump(reverse_result, file)
        else:
            with reverse_cache.open() as file:
                reverse_result = load(file)

        if "error" in reverse_result:  # pragma: no cover
            continue

        osm_id = reverse_result["osm_id"]
        osm_type = reverse_result["osm_type"][0].upper()

        if not details_cache.is_file():
            await sleep(1)

            details_url = f"{OSM_URL}/details.php?osmtype={osm_type}&osmid={osm_id}"
            details_url += "&addressdetails=1&format=json"
            async with AsyncClient() as client:
                response = await client.get(details_url, headers=HEADERS)
            details_result = response.json()

            with details_cache.open("w") as file:
                dump(details_result, file)

        if test_mode:  # pragma: no cover
            logger.info("Test mode enabled, stopping after first station")
            break
