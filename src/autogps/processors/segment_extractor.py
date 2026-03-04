"""
Orchestrator: 6-step pipeline to extract video segments for all collisions.
"""

import tempfile
from pathlib import Path
from typing import List

from ..core.config import Config
from ..parsers.collision_parser import CollisionSegment, discover_collision_files, parse_collision_file
from ..processors.mcap_reader import McapReader, TimeRange
from ..utils.ffmpeg_utils import FfmpegPipeEncoder, build_concat_list, require_ffmpeg, run_stream_copy
from ..utils.logger import get_logger


class SegmentExtractor:

    def __init__(self, config: Config) -> None:
        self.config = config
        self.reader = McapReader(Path(config.rosbag_path))
        self.logger = get_logger(__name__)

    def run(self) -> int:
        """
        Full pipeline. Returns number of successfully generated videos.

        Step 1: Validate + check ffmpeg
        Step 2: Discover + parse collision .txt files
        Step 3: Extract GPS trajectory (MCAP pass 1)
        Step 4: Resolve GPS coords → timestamp ranges
        Step 5: Extract video data (MCAP pass 2, single pass)
        Step 6: Assemble MP4 per collision via FFmpeg
        """
        require_ffmpeg()

        # Step 2
        segments = self._load_collision_segments()
        if not segments:
            self.logger.error("No collision segments found. Check collisions_folder in config.")
            return 0
        self.logger.info("Found %d collision segment(s) across all files", len(segments))

        # Step 3
        self.logger.info("Extracting GPS trajectory from rosbag...")
        trajectory = self.reader.extract_gps_trajectory(self.config.gps_topic)
        info = trajectory.get_info()
        self.logger.info(
            "GPS trajectory: %d points, %.1f s",
            info["num_points"], info.get("duration_s", 0),
        )

        # Step 4
        padding_ns = int(self.config.padding_seconds * 1e9)
        time_ranges: List[TimeRange] = []
        for seg in segments:
            for topic in self.config.camera_topics:
                ts_range = trajectory.get_timestamp_range(
                    seg.start_lat, seg.start_lon,
                    seg.end_lat, seg.end_lon,
                    tolerance_m=self.config.gps_tolerance_m,
                    padding_ns=padding_ns,
                )
                if ts_range is None:
                    self.logger.warning(
                        "Collision %d: GPS coordinates not matched in trajectory "
                        "(tolerance=%.1fm). Skipping.",
                        seg.collision_id, self.config.gps_tolerance_m,
                    )
                    continue
                output_path = self._build_output_path(seg, topic)
                time_ranges.append(TimeRange(
                    collision_id=seg.collision_id,
                    camera_topic=topic,
                    start_ns=ts_range[0],
                    end_ns=ts_range[1],
                    output_path=output_path,
                ))
                self.logger.debug(
                    "Collision %d [%s]: %.2f s window",
                    seg.collision_id, topic,
                    (ts_range[1] - ts_range[0]) / 1e9,
                )

        if not time_ranges:
            self.logger.error("No valid time ranges resolved. No videos will be generated.")
            return 0
        self.logger.info("Resolved %d/%d time range(s)", len(time_ranges), len(segments) * len(self.config.camera_topics))

        # Step 5 + 6
        if self.config.extraction.mode == "fast":
            return self._run_fast_path(time_ranges)
        else:
            return self._run_reencode_path(time_ranges)

    # ------------------------------------------------------------------
    # Fast path: JPEG stream copy
    # ------------------------------------------------------------------

    def _run_fast_path(self, time_ranges: List[TimeRange]) -> int:
        self.logger.info("Mode: fast (JPEG stream copy, no re-encoding)")
        success = 0

        with tempfile.TemporaryDirectory() as tmp:
            temp_dir = Path(tmp)

            # MCAP pass 2: extract all JPEG frames in one sweep
            jpeg_collections = self.reader.extract_jpeg_segments(time_ranges, temp_dir)

            # FFmpeg assembly per collision
            for tr in time_ranges:
                jpegs = jpeg_collections.get(tr.collision_id, {}).get(tr.camera_topic, [])
                if not jpegs:
                    self.logger.warning(
                        "Collision %d [%s]: 0 frames extracted, skipping.",
                        tr.collision_id, tr.camera_topic,
                    )
                    continue

                concat_file = temp_dir / f"concat_{tr.collision_id:03d}_{tr.camera_topic.replace('/', '_')}.txt"
                build_concat_list(jpegs, self.config.extraction.output_fps, concat_file)

                tr.output_path.parent.mkdir(parents=True, exist_ok=True)
                self.logger.info(
                    "Collision %d: encoding %d frames → %s",
                    tr.collision_id, len(jpegs), tr.output_path,
                )
                ok = run_stream_copy(concat_file, tr.output_path)
                if ok:
                    success += 1
                    self.logger.info("Collision %d: OK → %s", tr.collision_id, tr.output_path)
                else:
                    self.logger.error("Collision %d: FFmpeg failed.", tr.collision_id)

        return success

    # ------------------------------------------------------------------
    # Re-encode path
    # ------------------------------------------------------------------

    def _run_reencode_path(self, time_ranges: List[TimeRange]) -> int:
        self.logger.info("Mode: reencode (decode + H.264 encode)")

        # Optional rectification setup
        rectifier = None
        if self.config.extraction.rectify_enabled:
            try:
                import cv2
                import json
                import numpy as np
                with open(self.config.extraction.calibration_path) as f:
                    calib = json.load(f)
                # Load left camera calibration (used for first topic)
                mtx = np.array(calib["mtx_left"])
                dist = np.array(calib["dist_left"])
                # Rectification maps will be computed on first frame
                rectifier = {"mtx": mtx, "dist": dist, "map1": None, "map2": None}
            except Exception as e:
                self.logger.error("Failed to load calibration: %s. Rectification disabled.", e)

        # Build per-collision encoders lazily (need frame size from first frame)
        encoders: dict = {}
        frame_sizes: dict = {}
        success_set: set = set()
        procs: dict = {}

        try:
            import cv2
            import numpy as np

            for collision_id, topic, frame in self.reader.iter_decoded_frames(time_ranges):
                # Apply rectification if enabled
                if rectifier:
                    if rectifier["map1"] is None:
                        h, w = frame.shape[:2]
                        rectifier["map1"], rectifier["map2"] = cv2.initUndistortRectifyMap(
                            rectifier["mtx"], rectifier["dist"],
                            None, rectifier["mtx"], (w, h), cv2.CV_32FC1,
                        )
                    frame = cv2.remap(frame, rectifier["map1"], rectifier["map2"], cv2.INTER_LINEAR)

                key = (collision_id, topic)
                if key not in encoders:
                    h, w = frame.shape[:2]
                    tr = next((t for t in time_ranges if t.collision_id == collision_id and t.camera_topic == topic), None)
                    if tr is None:
                        continue
                    tr.output_path.parent.mkdir(parents=True, exist_ok=True)
                    enc = FfmpegPipeEncoder(
                        output_mp4=tr.output_path,
                        width=w, height=h,
                        fps=self.config.extraction.output_fps,
                        codec=self.config.extraction.codec,
                        crf=self.config.extraction.crf,
                        preset=self.config.extraction.preset,
                    )
                    enc.__enter__()
                    encoders[key] = enc
                    self.logger.info("Collision %d [%s]: encoding %dx%d", collision_id, topic, w, h)

                encoders[key].write_frame(frame)

        finally:
            for key, enc in encoders.items():
                enc.__exit__(None, None, None)
                collision_id, topic = key
                tr = next((t for t in time_ranges if t.collision_id == collision_id and t.camera_topic == topic), None)
                if tr and tr.output_path.exists() and tr.output_path.stat().st_size > 0:
                    success_set.add(key)
                    self.logger.info("Collision %d: OK → %s", collision_id, tr.output_path)
                else:
                    self.logger.error("Collision %d: output missing or empty.", collision_id)

        return len(success_set)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_collision_segments(self) -> List[CollisionSegment]:
        collision_files = discover_collision_files(Path(self.config.collisions_folder))
        if not collision_files:
            self.logger.warning("No collision_gps_*.txt files found in: %s", self.config.collisions_folder)
            return []
        self.logger.info("Found %d collision file(s)", len(collision_files))
        segments = []
        for f in collision_files:
            segs = parse_collision_file(f)
            if segs:
                self.logger.debug("%s: %d segment(s)", f.name, len(segs))
            else:
                self.logger.warning("%s: no valid segments parsed", f.name)
            segments.extend(segs)
        return segments

    def _build_output_path(self, seg: CollisionSegment, topic: str) -> Path:
        folder = Path(self.config.output_folder) / f"collision_{seg.collision_id:02d}"
        filename = f"collision_{seg.collision_id:02d}_{_slugify(topic)}.mp4"
        return folder / filename


def _slugify(topic: str) -> str:
    return topic.replace("/", "_").strip("_")
