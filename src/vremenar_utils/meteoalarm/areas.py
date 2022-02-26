"""MeteoAlarm areas utils."""
from io import BytesIO, TextIOWrapper
from json import load, dump
from pkgutil import get_data

from ..cli.common import CountryID
from ..database.stations import load_stations
from ..geo.polygons import point_in_polygon

from .common import AlertArea


async def process_meteoalarm_areas(
    country: CountryID, output: str, output_matches: str
) -> None:
    """Process MeteoAlarm ares."""
    with open('meteoalarm_geocodes.json') as f:
        data = load(f)

    areas: list[AlertArea] = []

    for feature in data['features']:
        properties = feature['properties']
        if properties['country'].lower() != country.value:
            continue

        coordinates = feature['geometry']['coordinates']
        polygons = []
        for polygon in coordinates:
            while isinstance(polygon, list) and len(polygon) == 1:
                polygon = polygon[0]
            polygons.append(polygon)

        area = AlertArea(properties['code'], properties['name'], polygons)
        print(area)
        areas.append(area)

    with open(output, 'w') as f:
        dump([area.to_dict() for area in areas], f)

    print(f'Total {len(areas)} areas')

    await match_meteoalarm_areas(country, output_matches, areas)


async def match_meteoalarm_areas(
    country: CountryID, output: str, areas: list[AlertArea]
) -> None:
    """Match MeteoAlarm areas with weather stations."""
    # load stations
    stations = await load_stations(country)
    # load overries
    overrides: dict[str, str] = {}
    overrides_data = get_data(
        'vremenar_utils', f'data/meteoalarm/{country.value}_overrides.json'
    )
    if overrides_data:
        bytes = BytesIO(overrides_data)
        with TextIOWrapper(bytes, encoding='utf-8') as file:
            overrides = load(file)

    matches: dict[str, str] = {}

    for id, station in stations.items():
        if 'country' in station and str(station['country']).lower() != country.value:
            continue

        label = station['name']
        coordinate = [float(station['longitude']), float(station['latitude'])]

        found = False
        for area in areas:
            for polygon in area.polygons:
                if point_in_polygon(coordinate, polygon):
                    matches[id] = area.code
                    found = True
                    break
            if found:
                break

        if not found:
            if id in overrides:
                matches[id] = overrides[id]
                continue
            raise ValueError(id, label, coordinate)

    with open(output, 'w') as f:
        dump(matches, f, indent=2)


def load_meteoalarm_areas(country: CountryID) -> list[AlertArea]:
    """Load MeteoAlarm areas from file."""
    areas: list[AlertArea] = []

    data = get_data('vremenar_utils', f'data/meteoalarm/{country.value}.json')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as file:
            areas_dict = load(file)

    for area_obj in areas_dict:
        areas.append(AlertArea.from_dict(area_obj))

    return areas
