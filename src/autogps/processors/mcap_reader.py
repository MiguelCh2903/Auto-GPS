"""
MCAP reader: GPS trajectory extraction and single-pass multi-collision JPEG extraction.
Uses rosbags (pure Python, no ROS needed).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

from tqdm import tqdm

from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from ..core.gps_trajectory import GPSTrajectory
from ..utils.logger import get_logger


@dataclass
class TimeRange:
    collision_id: int
    camera_topic: str
    start_ns: int
    end_ns: int
    output_path: Path


def _slugify(topic: str) -> str:
    """Convert a ROS topic to a safe filename part."""
    return topic.replace("/", "_").strip("_")


class McapReader:
    """
    Reads ROS2 .mcap files without ROS installed.
    Provides GPS trajectory extraction and efficient frame extraction.
    """

    def __init__(self, rosbag_path: Path) -> None:
        self.rosbag_path = rosbag_path
        self.typestore = get_typestore(Stores.ROS2_HUMBLE)
        self.logger = get_logger(__name__)

    # ------------------------------------------------------------------
    # GPS extraction
    # ------------------------------------------------------------------

    def extract_gps_trajectory(self, gps_topic: str) -> GPSTrajectory:
        """
        Read all GPS messages and return a GPSTrajectory.
        Single pass, only reads the GPS topic (fast, messages are tiny).
        """
        trajectory = GPSTrajectory()
        count = 0

        with AnyReader([self.rosbag_path], default_typestore=self.typestore) as reader:
            conns = [c for c in reader.connections if c.topic == gps_topic]
            if not conns:
                raise ValueError(
                    f"GPS topic '{gps_topic}' not found in rosbag.\n"
                    f"Available topics: {self._list_topics(reader)}"
                )

            total = sum(c.msgcount for c in conns)
            for connection, timestamp, rawdata in tqdm(
                reader.messages(connections=conns),
                total=total,
                desc="Reading GPS",
                unit="msg",
                leave=False,
            ):
                try:
                    msg = reader.deserialize(rawdata, connection.msgtype)
                    trajectory.add_point(
                        timestamp=timestamp,
                        lat=float(msg.latitude),
                        lon=float(msg.longitude),
                        alt=float(msg.altitude) if hasattr(msg, "altitude") else 0.0,
                    )
                    count += 1
                except Exception as e:
                    self.logger.debug("GPS deserialize error at ts=%d: %s", timestamp, e)

        trajectory.finalize_batch()
        self.logger.info("GPS trajectory: %d points", count)
        return trajectory

    # ------------------------------------------------------------------
    # Fast path: single-pass JPEG extraction (no decoding)
    # ------------------------------------------------------------------

    def extract_jpeg_segments(
        self,
        time_ranges: List[TimeRange],
        temp_dir: Path,
    ) -> Dict[int, Dict[str, List[Path]]]:
        """
        Single pass through the .mcap, extracting raw JPEG bytes for all
        time ranges simultaneously. No decoding, no numpy — pure file I/O.

        Returns: {collision_id: {camera_topic: [sorted list of jpeg paths]}}
        """
        if not time_ranges:
            return {}

        # Sort ranges by start for efficient sweep + early termination
        sorted_ranges = sorted(time_ranges, key=lambda r: r.start_ns)
        needed_topics: Set[str] = {tr.camera_topic for tr in sorted_ranges}
        global_start = sorted_ranges[0].start_ns
        global_end = max(r.end_ns for r in sorted_ranges)

        # Pre-create temp subdirectories and output structure
        jpeg_collections: Dict[int, Dict[str, List[Path]]] = {}
        frame_counters: Dict[Tuple[int, str], int] = {}
        for tr in sorted_ranges:
            jpeg_collections.setdefault(tr.collision_id, {}).setdefault(tr.camera_topic, [])
            frame_counters[(tr.collision_id, tr.camera_topic)] = 0
            d = temp_dir / f"col{tr.collision_id:03d}_{_slugify(tr.camera_topic)}"
            d.mkdir(parents=True, exist_ok=True)

        with AnyReader([self.rosbag_path], default_typestore=self.typestore) as reader:
            conns = [c for c in reader.connections if c.topic in needed_topics]
            if not conns:
                raise ValueError(
                    f"None of the camera topics found in rosbag: {needed_topics}\n"
                    f"Available: {self._list_topics(reader)}"
                )

            total = sum(c.msgcount for c in conns)
            with tqdm(total=total, desc="Extracting frames", unit="frame") as pbar:
                for connection, timestamp, rawdata in reader.messages(
                    connections=conns,
                    start=global_start,
                    stop=global_end,
                ):
                    pbar.update(1)
                    topic = connection.topic

                    for tr in sorted_ranges:
                        if tr.camera_topic != topic:
                            continue
                        if timestamp < tr.start_ns or timestamp > tr.end_ns:
                            continue

                        # In range — extract JPEG bytes without full deserialize
                        jpeg = self._extract_jpeg_bytes_raw(rawdata)
                        if jpeg is None:
                            # Fall back to deserialize
                            try:
                                msg = reader.deserialize(rawdata, connection.msgtype)
                                jpeg = bytes(msg.data)
                            except Exception as e:
                                self.logger.debug("Frame deserialize error: %s", e)
                                break

                        key = (tr.collision_id, topic)
                        idx = frame_counters[key]
                        frame_counters[key] = idx + 1

                        out_dir = temp_dir / f"col{tr.collision_id:03d}_{_slugify(topic)}"
                        frame_path = out_dir / f"frame_{idx:08d}.jpg"
                        frame_path.write_bytes(jpeg)
                        jpeg_collections[tr.collision_id][topic].append(frame_path)
                        break  # non-overlapping ranges per topic

        # Log frame counts
        for tr in sorted_ranges:
            n = len(jpeg_collections.get(tr.collision_id, {}).get(tr.camera_topic, []))
            self.logger.info("Collision %d [%s]: %d frames", tr.collision_id, tr.camera_topic, n)

        return jpeg_collections

    # ------------------------------------------------------------------
    # Re-encode path: decode frames for downstream processing
    # ------------------------------------------------------------------

    def iter_decoded_frames(
        self,
        time_ranges: List[TimeRange],
    ) -> Iterator[Tuple[int, str, "np.ndarray"]]:
        """
        Single-pass frame extraction yielding decoded BGR numpy frames.
        Yields (collision_id, camera_topic, bgr_frame).
        """
        try:
            import cv2
            import numpy as np
        except ImportError:
            raise ImportError(
                "opencv-python is required for extraction.mode = 'reencode'.\n"
                "Install with: pip install opencv-python"
            )

        if not time_ranges:
            return

        sorted_ranges = sorted(time_ranges, key=lambda r: r.start_ns)
        needed_topics: Set[str] = {tr.camera_topic for tr in sorted_ranges}
        global_start = sorted_ranges[0].start_ns
        global_end = max(r.end_ns for r in sorted_ranges)

        with AnyReader([self.rosbag_path], default_typestore=self.typestore) as reader:
            conns = [c for c in reader.connections if c.topic in needed_topics]
            total = sum(c.msgcount for c in conns)
            with tqdm(total=total, desc="Decoding frames", unit="frame") as pbar:
                for connection, timestamp, rawdata in reader.messages(
                    connections=conns,
                    start=global_start,
                    stop=global_end,
                ):
                    pbar.update(1)
                    topic = connection.topic

                    for tr in sorted_ranges:
                        if tr.camera_topic != topic:
                            continue
                        if timestamp < tr.start_ns or timestamp > tr.end_ns:
                            continue

                        try:
                            msg = reader.deserialize(rawdata, connection.msgtype)
                            data = np.frombuffer(bytes(msg.data), dtype=np.uint8)
                            frame = cv2.imdecode(data, cv2.IMREAD_COLOR)
                            if frame is not None:
                                yield tr.collision_id, topic, frame
                        except Exception as e:
                            self.logger.debug("Decode error: %s", e)
                        break

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def get_topic_list(self) -> List[str]:
        with AnyReader([self.rosbag_path], default_typestore=self.typestore) as reader:
            return self._list_topics(reader)

    def get_info(self) -> dict:
        with AnyReader([self.rosbag_path], default_typestore=self.typestore) as reader:
            topics = {}
            for c in reader.connections:
                topics[c.topic] = {"msgtype": c.msgtype, "msgcount": c.msgcount}
            duration_ns = reader.duration
            return {
                "path": str(self.rosbag_path),
                "duration_s": round(duration_ns / 1e9, 2) if duration_ns else None,
                "topics": topics,
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _list_topics(reader) -> List[str]:
        return sorted({c.topic for c in reader.connections})

    @staticmethod
    def _extract_jpeg_bytes_raw(rawdata: bytes) -> Optional[bytes]:
        """
        Fast CDR extraction of JPEG bytes from a CompressedImage message.
        CDR layout (little-endian):
          [0:4]   CDR header
          [4:8]   sec  (uint32)
          [8:12]  nanosec (uint32)
          [12:16] frame_id length (uint32)
          [16 + frame_id_len + pad] format string length + bytes
          [...] data array length (uint32) + JPEG bytes
        This is heuristic — if it fails, the caller falls back to full deserialize.
        """
        try:
            import struct
            pos = 4  # skip CDR header

            # Header.stamp (sec + nanosec)
            pos += 8

            # Header.frame_id: uint32 length + chars + null + padding to 4
            fid_len = struct.unpack_from("<I", rawdata, pos)[0]
            pos += 4 + fid_len
            # align to 4 bytes
            if pos % 4:
                pos += 4 - (pos % 4)

            # format string: uint32 length + chars + null + padding
            fmt_len = struct.unpack_from("<I", rawdata, pos)[0]
            pos += 4 + fmt_len
            if pos % 4:
                pos += 4 - (pos % 4)

            # data array: uint32 count + bytes
            data_len = struct.unpack_from("<I", rawdata, pos)[0]
            pos += 4
            return rawdata[pos: pos + data_len]
        except Exception:
            return None
