"""Geographic utility functions for GPS calculations."""

import math
from typing import Tuple

EARTH_RADIUS_M = 6371000.0


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two GPS points (meters). O(1)."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def validate_gps_coordinates(lat: float, lon: float) -> bool:
    return -90 <= lat <= 90 and -180 <= lon <= 180


def interpolate_gps(
    lat1: float, lon1: float, alt1: float, t1: int,
    lat2: float, lon2: float, alt2: float, t2: int,
    t_target: int,
) -> Tuple[float, float, float]:
    """Linear interpolation between two GPS points at a target timestamp."""
    if t2 == t1:
        return lat1, lon1, alt1
    alpha = max(0.0, min(1.0, (t_target - t1) / (t2 - t1)))
    return (
        lat1 + alpha * (lat2 - lat1),
        lon1 + alpha * (lon2 - lon1),
        alt1 + alpha * (alt2 - alt1),
    )
