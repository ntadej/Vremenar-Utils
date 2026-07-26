"""Polygon helper utilities."""

from shapely.geometry import (  # type: ignore # ruff: ignore[blanket-type-ignore]
    Point,
    Polygon,
)


def point_in_polygon(point: list[float], polygon: list[list[float]]) -> bool:
    """Check if a point is within a polygon."""
    p = Point(point)
    poly = Polygon(polygon)
    return bool(p.within(poly))
