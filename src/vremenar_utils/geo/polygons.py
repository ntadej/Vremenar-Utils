"""Polygon helper utilities."""
from shapely.geometry import Point, Polygon  # type: ignore
from typing import cast


def point_in_polygon(point: list[float], polygon: list[list[float]]) -> bool:
    """Check if a point is within a polygon."""
    p = Point(point)
    poly = Polygon(polygon)
    return cast(bool, p.within(poly))
