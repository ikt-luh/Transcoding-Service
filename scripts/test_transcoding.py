import csv
import time
from pathlib import Path
from rabbit import Transcoder, TranscoderConfig

def perform_experiments(experiment, config_map):
    """
    Runs one transcoding experiment based on the given config string.
    """
    config_key = experiment["config_name"]
    if config_key not in config_map:
        raise ValueError(f"Unknown config: {config_key}")

    out_path = Path(experiment["out_path"]) / "transcoded"
    out_path.mkdir(parents=True, exist_ok=True)

    csv_path = Path(experiment["csv_path"])
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    repeat = experiment["repeat"]
    input_dir = Path(experiment["input_dir"])
    seq = experiment["sequence"]
    seg_size = experiment["segment_size"]

    # Initialize transcoder with all known configs
    cfg_data = config_map[config_key]
    tx_config = TranscoderConfig(
        use_cuda=cfg_data["config"]["codec"] == "nvenc",
        geometry_qp=cfg_data["geoQP"],
        attribute_qp=cfg_data["attQP"],
        preset=cfg_data["config"]["attPreset"]
    )
    transcoder = Transcoder(tx_config)


    with open(csv_path, "a", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["sequence", "segment_size", "config", "repeat_idx", "segment_name", "duration_s"])

        print("Starting transcoding...")
        for r in range(repeat):
            for segment in sorted(input_dir.glob("seg_*.bin")):
                in_file = segment
                out_file = out_path / segment.name

                print(f"Transcoding {in_file.name} {config_key}")
                start = time.perf_counter()
                transcoder.transcode(str(in_file), str(out_file))
                duration = time.perf_counter() - start

                writer.writerow([seq, seg_size, config_key, r, segment.name, duration])
                f.flush()



if __name__ == "__main__":
    config_map = {
    "R1": {
        "config": {
            "codec": "nvenc",
            "attPreset": "p4",
            "geoPreset": "p4",
            "attEncThreads": 1,
            "geoEncThreads": 1,
            "gpu": 0
        },
        "geoQP": 20,
        "attQP": 27,
    }
}
    experiment = {
        "config_name": "R1",
        "out_path": "./results",
        "csv_path": "./results/experiment.csv",
        "repeat": 1,
        "n_thread": 2,
        "codec": "nvenc",
        "preset": "p4",
        "input_dir": "./media/encoded/30/soldier/5/",
        "sequence": "soldier",
        "segment_size": 60,
    }

    perform_experiments(experiment, config_map)