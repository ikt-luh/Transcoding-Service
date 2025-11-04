import os
import shutil
import logging
import subprocess
from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO)

TMC2_BIN = "/app/dependencies/mpeg-pcc-tmc2/bin/PccAppEncoder"
PREPARE_SCRIPT = "/app/prepare_media.py"


def prepare_media(cfg: dict, transcoder_pool: object, media_dir: Path):
    """
    Prepare media for streaming:
      - Verify and copy encoded sources
      - Generate MPDs if missing
      - Optionally transcode baseline representation to all others
    """
    prepare_segments(cfg, transcoder_pool)
    generate_mpd(cfg)


def generate_mpd(cfg):
    pass

def prepare_segments(cfg: dict, transcoder_pool: object):
    seq_config = cfg["sequences"]

    for seq, seq_dict in seq_config.items():
        base_config = seq_dict["base_config"]
        src_path = os.path.join(seq_dict["data_path"].format(cfg["gop_size"]), str(base_config))
        target_path = os.path.join(cfg["output_dir"], seq, str(base_config))
        os.makedirs(target_path, exist_ok=True)
        

        for file in os.listdir(src_path):
            if not file.endswith(".bin"):
                continue

            src_file = os.path.join(src_path, file)
            dst_file = os.path.join(target_path, file)
            if not os.path.exists(dst_file):
                shutil.copy2(src_file, dst_file)

            if cfg["baseline"]:
                for config_id in seq_dict["offered_configs"]:
                    target_path_transcoded = os.path.join(cfg["output_dir"], seq, str(config_id))
                    dst_file = os.path.join(target_path_transcoded, file)
                    os.makedirs(target_path_transcoded, exist_ok=True)

                    try:
                        print(config_id, src_file, dst_file)
                        transcoder_pool.submit(config_id, str(src_file), str(dst_file))
                    except Exception as e:
                        print("Failed to schedule")
