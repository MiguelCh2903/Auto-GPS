"""
Auto-GPS CLI entry point.

Usage:
  python -m autogps run [--config config.yaml] [--rosbag ...] [--collisions ...] [--mode fast|reencode]
  python -m autogps info <rosbag_path>
  python -m autogps parse-collisions <folder>
"""

import argparse
import sys
from pathlib import Path


def cmd_run(args) -> int:
    from .core.config import Config
    from .processors.segment_extractor import SegmentExtractor
    from .utils.logger import setup_logging

    cfg = Config.from_yaml(args.config)

    # CLI overrides
    if args.rosbag:
        cfg.rosbag_path = args.rosbag
    if args.collisions:
        cfg.collisions_folder = args.collisions
    if args.output:
        cfg.output_folder = args.output
    if args.mode:
        cfg.extraction.mode = args.mode

    setup_logging(cfg.log_level, cfg.log_file)

    try:
        cfg.validate()
    except ValueError as e:
        print(f"[ERROR] Config validation failed:\n{e}", file=sys.stderr)
        return 1

    extractor = SegmentExtractor(cfg)
    n = extractor.run()
    if n == 0:
        print("[WARNING] No videos were generated.", file=sys.stderr)
        return 1
    print(f"\nDone. Generated {n} video segment(s).")
    return 0


def cmd_info(args) -> int:
    from .processors.mcap_reader import McapReader
    from .utils.logger import setup_logging
    setup_logging("INFO")

    path = Path(args.rosbag)
    if not path.exists():
        print(f"[ERROR] Not found: {path}", file=sys.stderr)
        return 1

    reader = McapReader(path)
    info = reader.get_info()
    print(f"\nRosbag: {info['path']}")
    if info['duration_s']:
        print(f"Duration: {info['duration_s']} s")
    print("\nTopics:")
    for topic, meta in sorted(info['topics'].items()):
        print(f"  {topic:<60} {meta['msgcount']:>8} msgs  ({meta['msgtype']})")
    return 0


def cmd_parse_collisions(args) -> int:
    from .parsers.collision_parser import discover_collision_files, parse_collision_file
    from .utils.logger import setup_logging
    setup_logging("WARNING")

    folder = Path(args.folder)
    if not folder.exists():
        print(f"[ERROR] Folder not found: {folder}", file=sys.stderr)
        return 1

    files = discover_collision_files(folder)
    if not files:
        print(f"No collision_gps_*.txt files found in: {folder}")
        return 0

    total = 0
    for f in files:
        segs = parse_collision_file(f)
        print(f"\n{f.relative_to(folder)} — {len(segs)} collision(s)")
        for s in segs:
            print(
                f"  [{s.collision_id:3d}] START ({s.start_lat:.6f}, {s.start_lon:.6f})"
                f"  ->  END ({s.end_lat:.6f}, {s.end_lon:.6f})"
            )
        total += len(segs)

    print(f"\nTotal: {total} collision segment(s) in {len(files)} file(s)")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="autogps",
        description="Extract video segments from ROS2 rosbags using GPS collision coordinates",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Extract video segments for all collisions")
    p_run.add_argument("--config", "-c", default="config.yaml", help="Path to config.yaml")
    p_run.add_argument("--rosbag", help="Override rosbag_path from config")
    p_run.add_argument("--collisions", help="Override collisions_folder from config")
    p_run.add_argument("--output", help="Override output_folder from config")
    p_run.add_argument("--mode", choices=["fast", "reencode"], help="Override extraction mode")

    # info
    p_info = sub.add_parser("info", help="Show rosbag topics and duration")
    p_info.add_argument("rosbag", help="Path to .mcap rosbag file or directory")

    # parse-collisions
    p_parse = sub.add_parser("parse-collisions", help="List all detected collision segments (dry run)")
    p_parse.add_argument("folder", help="Path to collisions folder")

    args = parser.parse_args()

    dispatch = {
        "run": cmd_run,
        "info": cmd_info,
        "parse-collisions": cmd_parse_collisions,
    }
    sys.exit(dispatch[args.command](args))


if __name__ == "__main__":
    main()
