"""OSM Nominatim Utils."""

from json import dump, load
from pathlib import Path
from time import sleep

import httpx2

from vremenar_utils import __version__
from vremenar_utils.dwd.stations import load_stations_from_csv

OSM_CACHE_DIR: Path = Path.cwd() / ".cache/osm"
OSM_URL = "https://nominatim.openstreetmap.org"

HEADERS = {
    "user-agent": f"Vremenar-Utils/{__version__}",
    "referer": "https://vremenar.app",
}


def update_cache(input_file: Path) -> None:
    """Update Nominatim cache."""
    OSM_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Load stations
    stations: dict[str, dict[str, str | int | float]] = {}
    with input_file.open() as csv_file:
        stations = load_stations_from_csv(csv_file)

    # Loop over stations
    for station_id, station in stations.items():
        print(station_id, station)

        lat = station["lat"]
        lon = station["lon"]

        reverse_cache = OSM_CACHE_DIR / f"DWD:{station_id}:reverse.json"
        details_cache = OSM_CACHE_DIR / f"DWD:{station_id}:reverse_details.json"

        if not reverse_cache.is_file():
            sleep(1)

            reverse_url = (
                f"{OSM_URL}/reverse.php?lat={lat}&lon={lon}&zoom=16&format=jsonv2"
            )
            print(reverse_url)
            response = httpx2.get(reverse_url, headers=HEADERS)
            reverse_result = response.json()

            with reverse_cache.open("w") as file:
                dump(reverse_result, file)

            print(f"{reverse_cache} written")
        else:
            print(f"{reverse_cache} exists")
            with reverse_cache.open() as file:
                reverse_result = load(file)

        if "error" in reverse_result:
            print()
            continue

        osm_id = reverse_result["osm_id"]
        osm_type = reverse_result["osm_type"][0].upper()

        if not details_cache.is_file():
            sleep(1)

            details_url = f"{OSM_URL}/details.php?osmtype={osm_type}&osmid={osm_id}"
            details_url += "&addressdetails=1&format=json"
            print(details_url)
            response = httpx2.get(details_url, headers=HEADERS)
            details_result = response.json()

            with details_cache.open("w") as file:
                dump(details_result, file)

            print(f"{details_cache} written")
        else:
            print(f"{details_cache} exists")

        print()
