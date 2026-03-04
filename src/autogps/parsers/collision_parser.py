"""
Parser for collision_gps_XX.txt files.

State machine: IDLE → FOUND_ID → IN_START → IN_BETWEEN → IN_END → emit
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from ..utils.logger import get_logger

logger = get_logger(__name__)

_RE_COLLISION_ID = re.compile(r"COLLISION ID:\s*(\d+)")
_RE_GEO_COORDS = re.compile(r"Geographic Coordinates\s*:\s*([-\d.]+)\s*,\s*([-\d.]+)")


@dataclass
class CollisionSegment:
    collision_id: int
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    source_file: str


def parse_collision_file(txt_path: Path) -> List[CollisionSegment]:
    """
    Parse a collision GPS text file and return all CollisionSegment objects found.
    Skips malformed segments with a warning.
    """
    segments: List[CollisionSegment] = []

    # State machine states
    IDLE = 0
    FOUND_ID = 1
    IN_START = 2
    IN_BETWEEN = 3
    IN_END = 4

    state = IDLE
    collision_id: Optional[int] = None
    start_lat: Optional[float] = None
    start_lon: Optional[float] = None

    try:
        text = txt_path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        logger.warning("Cannot read %s: %s", txt_path, e)
        return segments

    for line in text.splitlines():
        stripped = line.strip()

        if state == IDLE:
            m = _RE_COLLISION_ID.search(stripped)
            if m:
                collision_id = int(m.group(1))
                state = FOUND_ID

        elif state == FOUND_ID:
            if "START" in stripped:
                state = IN_START

        elif state == IN_START:
            m = _RE_GEO_COORDS.search(stripped)
            if m:
                start_lat = float(m.group(1))
                start_lon = float(m.group(2))
                state = IN_BETWEEN

        elif state == IN_BETWEEN:
            if "END" in stripped and "Pole" in stripped:
                state = IN_END

        elif state == IN_END:
            m = _RE_GEO_COORDS.search(stripped)
            if m:
                end_lat = float(m.group(1))
                end_lon = float(m.group(2))
                if collision_id is not None and start_lat is not None and start_lon is not None:
                    segments.append(CollisionSegment(
                        collision_id=collision_id,
                        start_lat=start_lat,
                        start_lon=start_lon,
                        end_lat=end_lat,
                        end_lon=end_lon,
                        source_file=str(txt_path),
                    ))
                # Reset for next collision in the same file
                state = IDLE
                collision_id = None
                start_lat = None
                start_lon = None

    if state != IDLE and state != FOUND_ID:
        logger.warning("Incomplete collision block at end of %s (state=%d)", txt_path.name, state)

    return segments


def discover_collision_files(collisions_folder: Path) -> List[Path]:
    """Recursively find all collision_gps_*.txt files, sorted by path."""
    return sorted(collisions_folder.rglob("collision_gps_*.txt"))
