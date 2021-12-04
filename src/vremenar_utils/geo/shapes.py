"""Geo Shapes Utils."""
from geopandas import GeoDataFrame, read_file  # type: ignore
from io import BytesIO, TextIOWrapper
from pkgutil import get_data
from shapely.geometry import Point  # type: ignore
from typing import Tuple


def get_shape(id: str) -> None:
    """Get shape for a country."""
    # Taken from http://thematicmapping.org/downloads/world_borders.php
    # http://thematicmapping.org/downloads/TM_WORLD_BORDERS-0.3.zip
    gdf: GeoDataFrame = read_file('TM_WORLD_BORDERS-0.3.shp')
    gdf = gdf[gdf['ISO3'] == id]
    gdf.to_file(f'{id}.geojson', driver='GeoJSON')


def load_shape(country: str) -> Tuple[GeoDataFrame, GeoDataFrame]:
    """Load shape for a specific country."""
    data = get_data('vremenar_utils', f'data/shapes/{country}.json')
    if data:
        bytes = BytesIO(data)
        with TextIOWrapper(bytes, encoding='utf-8') as file:
            gdf = read_file(file)
            gdf_buffered = gdf.to_crs('EPSG:3857').buffer(2500).to_crs(gdf.crs)
            return (gdf, gdf_buffered)
    return (GeoDataFrame(), GeoDataFrame())


def inside_shape(point: Point, gdf: GeoDataFrame) -> bool:
    """Check if the point is inside the shape."""
    result = gdf.contains(point)[:]
    return result.iloc[0]
