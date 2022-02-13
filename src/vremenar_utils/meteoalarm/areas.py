"""MeteoAlarm areas utils."""
from io import BytesIO, TextIOWrapper
from json import load, dump
from pkgutil import get_data

from ..geo.polygons import point_in_polygon

from .common import AlarmArea, AlarmCountry, AlarmEncoder, load_stations


def process_meteoalarm_areas(
    country: AlarmCountry, output: str, output_matches: str
) -> None:
    """Process MeteoAlarm ares."""
    with open('meteoalarm_geocodes.json') as f:
        data = load(f)

    areas: list[AlarmArea] = []

    for feature in data['features']:
        properties = feature['properties']
        if properties['country'] != country.country_code():
            continue

        coordinates = feature['geometry']['coordinates']
        polygons = []
        for polygon in coordinates:
            while isinstance(polygon, list) and len(polygon) == 1:
                polygon = polygon[0]
            polygons.append(polygon)

        area = AlarmArea(properties['code'], properties['name'], polygons)
        print(area)
        areas.append(area)

    with open(output, 'w') as f:
        dump(areas, f, cls=AlarmEncoder)

    print(f'Total {len(areas)} areas')

    match_meteoalarm_areas(country, output_matches, areas)


def match_meteoalarm_areas(
    country: AlarmCountry, output: str, areas: list[AlarmArea]
) -> None:
    """Match MeteoAlarm areas with weather stations."""
    # load stations
    stations = load_stations(country)
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

    for s in stations:
        if country is AlarmCountry.Germany:
            s = stations[s]
            id = s['wmo_station_id']
            label = s['name']
            coordinate = [s['lon'], s['lat']]
        elif country is AlarmCountry.Slovenia:
            if s['country'] != country.country_code():
                continue
            id = s['id'].strip('_')
            label = s['title']
            coordinate = [s['longitude'], s['latitude']]

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
