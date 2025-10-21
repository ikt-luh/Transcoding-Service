#!/usr/bin/env python3

"""
This script encodes all sequences of the 8iVFBv2 dataset in the specified segment length.

Args:
    --config (str): YAML configuration file

Usage:
    python3 scripts/encode_8i.py --config configs/8i.yaml
"""

import os
import subprocess
import shutil
import logging
import yaml
import concurrent.futures
from pathlib import Path

logging.basicConfig(level=logging.INFO)

# Static encoder config paths (container-ready)
TMC2_BIN = "/app/dependencies/mpeg-pcc-tmc2/bin/PccAppEncoder"
CFG_BASE = Path("/app/dependencies/mpeg-pcc-tmc2/cfg")
CFG_COMMON = CFG_BASE / "common/ctc-common.cfg"
CFG_CONDITION = CFG_BASE / "condition/ctc-all-intra.cfg"
CFG_RATE = CFG_BASE / "rate/ctc-r5.cfg"

def extend_sequence(data_template: str, src_dir: Path, start_frame: int, src_frames: int, total_frames: int):
    extension = total_frames - src_frames
    if extension <= 0:
        return

    logging.info(f"[{src_dir}] Extending to {total_frames} frames")
    for i in range(extension):
        src_idx = src_frames - 1 - i
        dst_idx = src_frames + i
        src_file = src_dir / (data_template % (start_frame + src_idx))
        dst_file = src_dir / (data_template % (start_frame + dst_idx))
        shutil.copy2(src_file, dst_file)

def encode_segment_worker(args):
    part, offset, gop_size, ply_input, recon_out, out_path, seq_cfg = args

    cmd = [
        TMC2_BIN,
        f"--configurationFolder={CFG_BASE}/",
        f"--config={CFG_COMMON}",
        f"--config={CFG_CONDITION}",
        f"--config={seq_cfg}",
        f"--config={CFG_RATE}",
        "--profileReconstructionIdc=0",
        f"--uncompressedDataPath={ply_input}",
        f"--startFrameNumber={offset}",
        f"--frameCount={gop_size}",
        f"--groupOfFramesSize={gop_size}",
        f"--reconstructedDataPath={recon_out}",
        f"--compressedStreamPath={out_path}",
    ]

    logging.info(f"[{out_path.name}] Encoding segment {part}")
    subprocess.run(cmd, check=True)

def encode_sequence(seq: str, cfg: dict, out_base: Path, gop_size: int, total_frames: int):
    start_frame = cfg["start_frame"]
    src_frames = cfg["num_frames"]
    src_dir = Path(cfg["data_path"])
    extend_sequence(cfg["data_template"], src_dir, start_frame, src_frames, total_frames)

    rep_id = cfg["encoding_config"]
    out_dir = out_base / seq / rep_id
    out_dir.mkdir(parents=True, exist_ok=True)

    num_segments = total_frames // gop_size
    ply_input = str(src_dir / cfg["data_template"])
    recon_out = f"{seq}_rec_%04d.ply"
    seq_cfg = Path(cfg["sequence_cfg"])

    segment_args = []
    for part in range(num_segments):
        offset = start_frame + part * gop_size
        out_file = f"seg_{part:03d}.bin"
        out_path = out_dir / out_file
        segment_args.append((part, offset, gop_size, ply_input, recon_out, out_path, seq_cfg))

    max_workers = min(os.cpu_count(), 16)
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(encode_segment_worker, segment_args))

def run(config_path: Path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    out_root = Path(config["output_dir"])
    durations = config["duration"]  # list like [30, 60, 120]
    sequences = config["sequences"]
    total_frames = 600

    for duration in durations:
        gop_size = int(duration)  # duration is in frames
        out_base = out_root / f"{duration}"

        for seq, cfg in sequences.items():
            logging.info(f"Encoding sequence {seq} at GOP size {gop_size}")
            encode_sequence(seq, cfg, out_base, gop_size, total_frames)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Encode all 8iVFBv2 sequences at multiple durations.")
    parser.add_argument("config", type=str, help="Path to YAML config file")
    args = parser.parse_args()

    run(Path(args.config))