import os
import asyncio
import shutil
import subprocess
from pathlib import Path
import yaml


TMC2_BIN = "/app/dependencies/mpeg-pcc-tmc2/bin/PccAppEncoder"
PREPARE_SCRIPT = "/app/prepare_media.py"


async def prepare_media(cfg: dict, transcoder_pool: object, media_dir: Path):
    """
    Prepare media for streaming:
      - Verify and copy encoded sources
      - Generate MPDs if missing
      - Optionally transcode baseline representation to all others
    """
    await prepare_segments(cfg, transcoder_pool)
    generate_mpd(cfg)


def generate_mpd(cfg):
    """
    Generate MPD manifests for all sequences and representations in cfg.
    This reuses the same XML templates as the original prepare script.
    """
    mpd_template_path = Path(cfg["mpd_template"])
    rep_template_path = Path(cfg["representation_template"])

    with open(mpd_template_path, "r") as f:
        mpd_template = f.read()
    with open(rep_template_path, "r") as f:
        rep_template = f.read()

    out_base = Path(cfg["output_dir"])
    gop_size = cfg["gop_size"]
    total_frames = cfg["total_frames"]
    timescale = cfg.get("timescale", 1)
    coding_config = cfg["transcoder"]

    for seq, seq_dict in cfg["sequences"].items():
        seq_dir = out_base / seq
        representations = []
        bandwidths = {}

        for rep_id in [seq_dict["base_config"]] + seq_dict.get("offered_configs", []):
            num_segments = total_frames // gop_size
            timeline_entries = [
                f'<S t="{i * gop_size}" d="{gop_size}"/>'
                for i in range(num_segments)
            ]
            timeline_str = "\n            ".join(timeline_entries)

            # get file size
            repr_folder = os.path.join(cfg["output_dir"], seq, str(rep_id))
            if os.path.exists(repr_folder) and rep_id == seq_dict["base_config"]:
                total_bytes = sum(
                    os.path.getsize(os.path.join(repr_folder, f))
                    for f in os.listdir(repr_folder)
                    if f.endswith(".bin")
                )
                duration_sec = total_frames / timescale  # e.g. 600 frames / 30 = 20 s
                bw_bps = int((total_bytes * 8) / duration_sec)
                bandwidths[rep_id] = bw_bps
            else:
                # If the folder does not exist, we will do an estimate using repr 5
                base_folder = os.path.join(cfg["output_dir"], seq, str(seq_dict["base_config"]))
                base_bytes = sum(
                    os.path.getsize(os.path.join(base_folder, f))
                    for f in os.listdir(base_folder)
                    if f.endswith(".bin")
                )
                duration_sec = total_frames / timescale
                base_bw_bps = int((base_bytes * 8) / duration_sec)

                factor = coding_config[str(rep_id)].get("bw_estimate_factor", 1.0)
                bw_bps = int(base_bw_bps * factor)
                bandwidths[rep_id] = bw_bps

    
            
            rep = rep_template
            rep = rep.replace("$REP_ID$", rep_id)
            rep = rep.replace("$BANDWIDTH$", str(bandwidths.get(rep_id, 1000000)))
            rep = rep.replace("$SEG_DURATION$", str(gop_size))
            rep = rep.replace("$TIMESCALE$", str(timescale))
            rep = rep.replace("$SEGMENT_TIMELINE$", timeline_str)
            rep = rep.replace("$RepresentationID$", rep_id)

            representations.append(rep)

        mpd_output = mpd_template
        mpd_output = mpd_output.replace("$DURATION$", str(total_frames // timescale))
        mpd_output = mpd_output.replace("$REPRESENTATIONS$", "\n".join(representations))

        mpd_path = seq_dir / f"{seq}.mpd"
        seq_dir.mkdir(parents=True, exist_ok=True)
        with open(mpd_path, "w") as f:
            f.write(mpd_output)

        print(f"[{seq}] MPD written to {mpd_path}")

async def prepare_segments(cfg: dict, transcoder_pool: object):
    seq_config = cfg["sequences"]

    for seq, seq_dict in seq_config.items():
        base_config = seq_dict["base_config"]
        pre_cfg = seq_dict.get("pre_encoded")
        src_path = os.path.join(seq_dict["data_path"].format(cfg["gop_size"]), str(base_config))
        target_path = os.path.join(cfg["output_dir"], seq, str(base_config))
        
        for config_id in [base_config] + seq_dict.get("offered_configs", []):
            target_path_transcoded = os.path.join(cfg["output_dir"], seq, str(config_id))
            os.makedirs(target_path_transcoded, exist_ok=True)
            init_file = os.path.join(target_path_transcoded, "init.bin")
            Path(init_file).touch()


        for file in os.listdir(src_path):
            if not file.endswith(".bin"):
                continue

            src_file = os.path.join(src_path, file)
            dst_file = os.path.join(target_path, file)
            if not os.path.exists(dst_file):
                shutil.copy2(src_file, dst_file)

            if pre_cfg:
                target_folder = os.path.join(cfg["output_dir"], seq, pre_cfg)
                dst_file_low = os.path.join(target_folder, file)
                if not os.path.exists(dst_file_low):
                    print(f"[prepare] Pre-encode seq={seq} file={file} → repr={pre_cfg}")
                    job_id = transcoder_pool.submit(pre_cfg, src_file, dst_file_low)
                    ok = await transcoder_pool.wait_job(job_id, timeout=120)
                    if not ok:
                        raise RuntimeError(f"Pre-encoding timeout: {seq}/{file} to repr {pre_cfg}")


            if cfg["baseline"]:
                job_ids = []
                for config_id in seq_dict["offered_configs"]:
                    target_path_transcoded = os.path.join(cfg["output_dir"], seq, str(config_id))
                    dst_file = os.path.join(target_path_transcoded, file)
                    os.makedirs(target_path_transcoded, exist_ok=True)

                    if not os.path.exists(dst_file):
                        try:
                            print(config_id, src_file, dst_file)
                            job_id = transcoder_pool.submit(config_id, str(src_file), str(dst_file))
                            job_ids.append(job_id)
                        except Exception as e:
                            print("Failed to schedule")
                    else:
                        print(f"Path {dst_file} exists.")

                # Wait for all submitted jobs for this segment
                for job_id in job_ids:
                    ok = await transcoder_pool.wait_job(job_id, timeout=60)
                    if not ok:
                        raise RuntimeError(f"Transcoding timeout for job {job_id}")