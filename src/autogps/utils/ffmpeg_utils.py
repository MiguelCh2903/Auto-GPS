"""FFmpeg helpers: stream-copy concat and re-encode pipe."""

import shutil
import subprocess
from pathlib import Path
from typing import Iterator, List, Optional

import numpy as np

from .logger import get_logger

logger = get_logger(__name__)


def check_ffmpeg() -> bool:
    """Return True if ffmpeg is available on PATH."""
    return shutil.which("ffmpeg") is not None


def require_ffmpeg() -> None:
    if not check_ffmpeg():
        raise RuntimeError(
            "ffmpeg not found on PATH.\n"
            "Install it from https://ffmpeg.org/download.html and make sure it is in your PATH."
        )


# ---------------------------------------------------------------------------
# Option A: JPEG concat → stream copy (no re-encoding)
# ---------------------------------------------------------------------------

def build_concat_list(jpeg_paths: List[Path], fps: float, concat_file: Path) -> None:
    """Write an FFmpeg concat demuxer input list."""
    duration = 1.0 / fps
    lines = ["ffconcat version 1.0\n"]
    for p in jpeg_paths:
        # Use forward slashes; works on both Windows and Linux with -safe 0
        lines.append(f"file '{p.as_posix()}'\n")
        lines.append(f"duration {duration:.8f}\n")
    concat_file.write_text("".join(lines), encoding="utf-8")


def run_stream_copy(concat_file: Path, output_mp4: Path) -> bool:
    """
    Mux JPEG frames into MP4 using FFmpeg concat demuxer with stream copy.
    No re-encoding — fastest possible output.
    """
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-c:v", "copy",
        "-movflags", "+faststart",
        str(output_mp4),
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        logger.debug("ffmpeg stderr:\n%s", result.stderr.decode(errors="replace"))
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Option B: raw BGR pipe → H.264 re-encode
# ---------------------------------------------------------------------------

class FfmpegPipeEncoder:
    """Context manager that writes BGR numpy frames to an FFmpeg H.264 process."""

    def __init__(
        self,
        output_mp4: Path,
        width: int,
        height: int,
        fps: float,
        codec: str = "libx264",
        crf: int = 23,
        preset: str = "veryfast",
    ) -> None:
        self.output_mp4 = output_mp4
        self.width = width
        self.height = height
        self.fps = fps
        self.codec = codec
        self.crf = crf
        self.preset = preset
        self._proc: Optional[subprocess.Popen] = None

    def __enter__(self) -> "FfmpegPipeEncoder":
        cmd = [
            "ffmpeg", "-y",
            "-f", "rawvideo",
            "-vcodec", "rawvideo",
            "-s", f"{self.width}x{self.height}",
            "-pix_fmt", "bgr24",
            "-r", str(self.fps),
            "-i", "-",
            "-an",
            "-vcodec", self.codec,
            "-crf", str(self.crf),
            "-preset", self.preset,
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            str(self.output_mp4),
        ]
        self._proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.DEVNULL)
        return self

    def write_frame(self, frame: np.ndarray) -> None:
        assert self._proc and self._proc.stdin
        self._proc.stdin.write(frame.tobytes())

    def __exit__(self, *_) -> None:
        if self._proc and self._proc.stdin:
            self._proc.stdin.close()
        if self._proc:
            self._proc.wait()
