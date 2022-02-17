"""MeteoAlarm areas utils."""
from io import BytesIO, TextIOWrapper
from json import load, dump
from pkgutil import get_data

from ..geo.polygons import point_in_polygon

from .common import AlertArea, AlertCountry, AlertEncoder, load_stations


def process_meteoalarm_areas(
    country: AlertCountry, output: str, output_matches: str
) -> None:
    """Process MeteoAlarm ares."""
    with open('meteoalarm_geocodes.json') as f:
        data = load(f)

    areas: list[AlertArea] = []

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

        area = AlertArea(properties['code'], properties['name'], polygons)
        print(area)
        areas.append(area)

    with open(output, 'w') as f:
        dump(areas, f, cls=AlertEncoder)

    print(f'Total {len(areas)} areas')

    match_meteoalarm_areas(country, output_matches, areas)


def match_meteoalarm_areas(
    country: AlertCountry, output: str, areas: list[AlertArea]
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

    for station in stations:
        if country is AlertCountry.Germany:
            s = stations[station]
            id = str(s['wmo_station_id'])
            label = s['name']
            coordinate = [float(s['lon']), float(s['lat'])]
        elif country is AlertCountry.Slovenia:
            s = stations[station]
            if s['country'] != country.country_code():
                continue
            id = str(s['id']).strip('_')
            label = s['title']
            coordinate = [float(s['longitude']), float(s['latitude'])]

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
