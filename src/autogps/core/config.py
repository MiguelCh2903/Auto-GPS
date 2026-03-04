"""Configuration dataclasses and YAML loader."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class ExtractionConfig:
    mode: str = "fast"               # "fast" | "reencode"
    rectify_enabled: bool = False
    calibration_path: str = "stereo_calib.json"
    codec: str = "libx264"
    crf: int = 23
    preset: str = "veryfast"
    output_fps: float = 24.0


@dataclass
class Config:
    rosbag_path: str = ""
    collisions_folder: str = "output"
    output_folder: str = "output"
    camera_topics: List[str] = field(
        default_factory=lambda: ["/camera_left/image_raw/compressed"]
    )
    gps_topic: str = "/swift/navsat_fix"
    gps_tolerance_m: float = 50.0
    padding_seconds: float = 2.0
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    log_level: str = "INFO"
    log_file: Optional[str] = None

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "Config":
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        extraction_data = data.pop("extraction", {})
        obj = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        obj.extraction = ExtractionConfig(
            **{k: v for k, v in extraction_data.items() if k in ExtractionConfig.__dataclass_fields__}
        )
        return obj

    def validate(self) -> None:
        if not Path(self.rosbag_path).exists():
            raise ValueError(f"rosbag_path does not exist: {self.rosbag_path}")
        if not Path(self.collisions_folder).exists():
            raise ValueError(f"collisions_folder does not exist: {self.collisions_folder}")
        if self.extraction.mode not in ("fast", "reencode"):
            raise ValueError("extraction.mode must be 'fast' or 'reencode'")
        if (
            self.extraction.mode == "reencode"
            and self.extraction.rectify_enabled
            and not Path(self.extraction.calibration_path).exists()
        ):
            raise ValueError(
                f"calibration_path not found: {self.extraction.calibration_path}\n"
                "Required when extraction.rectify_enabled = true"
            )
