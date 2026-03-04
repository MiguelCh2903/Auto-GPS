"""GPS trajectory with binary search, Haversine matching, and timestamp range resolution."""

import bisect
from dataclasses import dataclass
from typing import List, Optional, Tuple

from ..utils.geo_utils import haversine_distance, validate_gps_coordinates
from ..utils.logger import get_logger


@dataclass
class GPSPoint:
    timestamp: int  # nanoseconds
    lat: float
    lon: float
    alt: float = 0.0


class GPSTrajectory:
    """
    GPS trajectory with O(log n) lookup and O(n) segment matching.
    Uses batch insertion (sort once) for O(n log n) build cost.
    """

    def __init__(self) -> None:
        self.points: List[GPSPoint] = []
        self._timestamps: List[int] = []   # kept in sync, sorted
        self._batch: List[GPSPoint] = []
        self.logger = get_logger(__name__)

    # ------------------------------------------------------------------
    # Building the trajectory
    # ------------------------------------------------------------------

    def add_point(self, timestamp: int, lat: float, lon: float, alt: float = 0.0) -> bool:
        """Accumulate a point (call finalize_batch() when done)."""
        if not validate_gps_coordinates(lat, lon):
            return False
        self._batch.append(GPSPoint(timestamp, lat, lon, alt))
        return True

    def finalize_batch(self) -> None:
        """Sort accumulated points once and merge into main list. O(n log n)."""
        if not self._batch:
            return
        self._batch.sort(key=lambda p: p.timestamp)
        self.points.extend(self._batch)
        self._timestamps.extend(p.timestamp for p in self._batch)
        self._batch = []

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_timestamp_range(
        self,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float,
        tolerance_m: float = 50.0,
        padding_ns: int = 0,
    ) -> Optional[Tuple[int, int]]:
        """
        Find (start_ns, end_ns) for the trajectory segment closest to the
        given GPS coordinates, within tolerance_m.

        Returns None if no match found. Applies padding_ns before/after.
        """
        if not self.points:
            return None

        result = self._find_segment_indices(start_lat, start_lon, end_lat, end_lon, tolerance_m)
        if result is None:
            return None

        start_idx, end_idx = result
        start_ns = max(0, self.points[start_idx].timestamp - padding_ns)
        end_ns = self.points[end_idx].timestamp + padding_ns
        return start_ns, end_ns

    def _find_segment_indices(
        self,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float,
        tolerance_m: float,
    ) -> Optional[Tuple[int, int]]:
        """O(n) single-pass best-match search for start then end."""
        start_idx: Optional[int] = None
        end_idx: Optional[int] = None
        min_start_dist = float("inf")
        min_end_dist = float("inf")

        for i, pt in enumerate(self.points):
            if start_idx is None:
                d = haversine_distance(pt.lat, pt.lon, start_lat, start_lon)
                if d < min_start_dist:
                    min_start_dist = d
                    if d <= tolerance_m:
                        start_idx = i

            if start_idx is not None and i >= start_idx:
                d = haversine_distance(pt.lat, pt.lon, end_lat, end_lon)
                if d < min_end_dist:
                    min_end_dist = d
                    if d <= tolerance_m:
                        end_idx = i

        if start_idx is None or end_idx is None:
            closest_start = f"{min_start_dist:.1f}m"
            closest_end = f"{min_end_dist:.1f}m"
            self.logger.debug(
                "Segment not found. Closest start: %s, closest end: %s (tolerance: %.1fm)",
                closest_start, closest_end, tolerance_m,
            )
            return None

        return start_idx, end_idx

    # ------------------------------------------------------------------
    # Info
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.points)

    def get_info(self) -> dict:
        if not self.points:
            return {"num_points": 0}
        duration_s = (self.points[-1].timestamp - self.points[0].timestamp) / 1e9
        return {
            "num_points": len(self.points),
            "duration_s": round(duration_s, 2),
            "start_ns": self.points[0].timestamp,
            "end_ns": self.points[-1].timestamp,
        }
