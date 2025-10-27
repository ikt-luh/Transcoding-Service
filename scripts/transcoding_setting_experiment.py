import sys
import yaml
import csv
import time
from pathlib import Path
from transcoder import Transcoder

def perform_experiments(experiment):
    """
    Runs one transcoding experiment with the parameters and configuration given in the dictionary.
    Each segment's time for each repeat is logged in .csv.
    Additionally, transcoded bitstreams are saved once per configuration to file for later processing.
    """
    out_path = experiment["out_path"]
    n_threads = experiment["n_thread"]
    codec = experiment["codec"]
    preset = experiment["preset"]
    repeat = experiment["repeat"]
    csv_path = experiment["csv_path"]
    input_dir = experiment["input_dir"]
    seq = experiment["sequence"]
    seg_size = experiment["segment_size"]
    #rate = experiment["rate"]
    
    # Transcoder Configuration
    transcoder_config = {
        "codec": codec,
        "attPreset": preset,
        "geoPreset": preset,
        "attEncThreads": n_threads,
        "geoEncThreads": n_threads,
        "attEncodingMode": "AI",
        "geoEncodingMode": "AI",
        "attQP": 27, #TODO
        "geoQP": 20, #TODO
        #"attQP": rate["att"],
        #"geoQP": rate["geo"]
    }

    out_dir = Path(out_path) / "transcoded"
    out_dir.mkdir(parents=True, exist_ok=True)

    t = Transcoder()

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "a", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["sequence", "segment_size", "codec", "preset", "repeat_idx", "segment_name", "duration_s"])
        print("Starting repeats")

        for r in range(repeat):
            for segment in sorted(input_dir.glob("seg_*.bin")):
                in_file = segment
                out_file = out_dir / segment.name

                print(f"TRANSCODING {in_file} to {out_file}")

                start = time.perf_counter()
                t.transcode(str(in_file), str(out_file), transcoder_config)
                duration = time.perf_counter() - start

                writer.writerow([seq, seg_size, codec, preset, r, segment.name, duration])
                f.flush()  # Ensure data is written even on long runs



def get_experiments_from_config(config: dict):
    """
    Generates the experiment configurations from the config dict loaded from yaml.

    Parameters:
        config (dict): The raw dict loaded from the config.yaml file

    Returns:
        experiments (list): A list of experiment dictionaries that contain the exact parameters of the experiment to run.
    """
    base_path = Path(config["encoded_data"]["base"])
    out_dir = Path(config["out_dir"])
    num_repeats = config["num_repeats"]
    sequences = config["encoded_data"]["sequences"]
    segment_sizes = config["encoded_data"]["segment_sizes"]

    experiments = []

    for seq in sequences:
        for size in segment_sizes:
            for n_thread in config["n_threads"]:
                for codec, presets in config["codecs"].items():
                    for preset in presets["preset"]:
                        input_dir = base_path / str(size) / seq / "5"
                        out_path = out_dir / f"{seq}_{size}_{codec}_n{n_thread}_{preset}"
                        csv_path = out_path / "results.csv"

                        experiment = {
                            "sequence": seq,
                            "input_dir": input_dir,
                            "segment_size": size,
                            "out_path": out_path,
                            "csv_path": csv_path,
                            "codec": codec,
                            "preset": preset,
                            "n_thread": n_thread,
                            "repeat": num_repeats
                        }
                        experiments.append(experiment)

    return experiments

def run(config_path: Path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    out_dir = Path(config["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    experiments = get_experiments_from_config(config)
    print(len(experiments))

    for i, experiment in enumerate(experiments):
        print(f"Running Experiment {i}")
        print(experiment)
        perform_experiments(experiment)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run transcoding experiments on 8iVFBv2 encoded segments.")
    parser.add_argument("config", type=str, help="Path to YAML config file")
    args = parser.parse_args()

    run(Path(args.config))
