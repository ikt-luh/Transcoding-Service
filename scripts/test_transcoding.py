import csv
import time
import yaml
import argparse
from pathlib import Path
from itertools import product
from rabbit import Transcoder, TranscoderConfig


def perform_experiments(config):
    base_input = Path(config["base_input_dir"])
    output_dir = Path(config["output_dir"])
    csv_path = Path(config["csv_path"])
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    repeat = config["repeat"]
    sequences = config["sequences"]
    segment_sizes = config["segment_sizes"]
    codecs = config["codecs"]
    rate_settings = config["rate_settings"]

    with open(csv_path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            "sequence", "segment_size", "rate_key",
            "codec", "preset",
            "geoQP", "attQP",
            "repeat_idx", "segment_name", "duration_s"
        ])

        for seq, seg_size, rate_key in product(sequences, segment_sizes, rate_settings.keys()):
            rates = rate_settings[rate_key]
            geo_qp, att_qp = rates["geoQP"], rates["attQP"]

            for codec, codec_data in codecs.items():
                for preset in codec_data["presets"]:
                    print(f"\n=== {seq} | seg={seg_size} | {rate_key} | {codec} | {preset} | g={geo_qp} a={att_qp} ===")

                    tx_config = TranscoderConfig(
                        use_cuda=(codec == "nvenc"),
                        geometry_qp=geo_qp,
                        attribute_qp=att_qp,
                        preset=preset,
                    )
                    transcoder = Transcoder(tx_config)

                    input_dir = base_input / str(seg_size) / seq / "5"
                    output_path = output_dir / seq / f"{codec}_{preset}_{rate_key}_seg{seg_size}"
                    output_path.mkdir(parents=True, exist_ok=True)

                    for r in range(repeat):
                        for segment in sorted(input_dir.glob("seg_*.bin")):
                            in_file = segment
                            out_file = output_path / segment.name

                            start = time.perf_counter()
                            transcoder.transcode(in_file, out_file)
                            duration = time.perf_counter() - start

                            writer.writerow([
                                seq, seg_size, rate_key,
                                codec, preset,
                                geo_qp, att_qp,
                                r, segment.name, duration
                            ])
                            f.flush()

                    transcoder.close()


def main():
    parser = argparse.ArgumentParser(description="Run point cloud transcoding experiments.")
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the experiment configuration YAML file."
    )
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    perform_experiments(config)


if __name__ == "__main__":
    main()
