import time
import os
import uuid
import threading
import multiprocessing
from queue import Empty

from rabbit import Transcoder, TranscoderConfig

def _worker_loop(cfg_id: str, cfg_dict: dict, gpu_id: int, queue: multiprocessing.Queue):
    """Worker loop: run transcoder jobs for one configuration on one GPU."""
    try:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
        print(f"[TranscoderConfig] CUDA_VISIBLE_DEVICES={os.environ['CUDA_VISIBLE_DEVICES']}")

        transcoder = Transcoder()
        tx_cfg = TranscoderConfig(
            use_cuda=cfg_dict.get("cuda", False),
            geometry_qp=cfg_dict.get("geoQP", 32),
            attribute_qp=cfg_dict.get("attQP", 32),
            preset=cfg_dict.get("preset", "ultrafast"),
            gpuID=0,
        )
        transcoder.set_config(tx_cfg)
    except Exception as e:
        print(f"[Worker-{cfg_id}][GPU {gpu_id}] Failed to init transcoder: {e}", flush=True)
        return

    print(f"[Worker-{cfg_id}][GPU {gpu_id}] ready.")
    while True:
        item = queue.get()
        if item is None:
            print(f"[Worker-{cfg_id}][GPU {gpu_id}] shutting down.")
            break

        job_id, src_path, out_path = item
        try:
            print(f"[Worker-{cfg_id}][GPU {gpu_id}] Transcoding {src_path} to {out_path}", flush=True)
            transcoder.transcode(src_path, out_path)
        except Exception as e:
            print(f"[Worker-{cfg_id}][GPU {gpu_id}] Error on job {job_id}: {e}", flush=True)


class TranscoderPool:
    """Multi-GPU transcoder pool managing per-configuration queues and workers."""

    def __init__(self, gpu_plan: dict[int, int], configs: dict[str, dict]):
        self.gpu_plan = gpu_plan or {0: 1}
        self.configs = configs
        self.queues = {cfg_id: multiprocessing.Queue(maxsize=100) for cfg_id in configs}
        self.procs = {cfg_id: [] for cfg_id in configs}
        self.lock = threading.Lock()

    def start(self):
        cfg_ids = list(self.configs.keys())
        num_cfgs = len(cfg_ids)
        total_capacity = sum(self.gpu_plan.values())

        if total_capacity < num_cfgs:
            raise ValueError(f"Total capacity ({total_capacity}) < number of configs ({num_cfgs})")

        # distribute evenly: each config gets at least floor(total_capacity / num_cfgs) workers
        base_per_config = total_capacity // num_cfgs
        remainder = total_capacity % num_cfgs

        print(f"[Pool] Spawning workers: total {total_capacity} across {len(self.gpu_plan)} GPUs")
        print(f"[Pool] Each config gets {base_per_config} (+1 for {remainder} configs) workers")

        gpu_slots = []
        for gpu_id, cap in self.gpu_plan.items():
            gpu_slots.extend([gpu_id] * cap)

        # assign workers to configs round-robin over gpu_slots
        idx = 0
        total_spawned = 0
        for i, cfg_id in enumerate(cfg_ids):
            cfg = self.configs[cfg_id]
            num_workers = base_per_config + (1 if i < remainder else 0)

            for _ in range(num_workers):
                gpu_id = gpu_slots[idx % len(gpu_slots)]
                p = multiprocessing.Process(
                    target=_worker_loop,
                    args=(cfg_id, cfg, gpu_id, self.queues[cfg_id]),
                )
                p.start()
                #time.sleep(0.2) # Avoid race conditions?
                self.procs[cfg_id].append(p)
                print(f"[Pool] Started worker {cfg_id} on GPU {gpu_id}")
                total_spawned += 1
                idx += 1

        print(f"[Pool] Started {total_spawned} transcoder workers across GPUs {list(self.gpu_plan.keys())}")

    def stop(self):
        """Graceful shutdown of all workers."""
        for q in self.queues.values():
            for _ in range(sum(self.gpu_plan.values())):
                q.put(None)
        for plist in self.procs.values():
            for p in plist:
                p.join()
        print("All transcoder workers stopped.")

    def submit(self, cfg_id: str, src_path: str, out_path: str):
        """Enqueue a job for the specific configuration."""
        if cfg_id not in self.queues:
            raise KeyError(f"No queue for config {cfg_id}")
        job_id = str(uuid.uuid4())
        with self.lock:
            try:
                self.queues[cfg_id].put_nowait((job_id, src_path, out_path))
                print(f"[Submit] Job {job_id} queued for cfg {cfg_id}")
            except Exception:
                raise ValueError(f"Queue for config {cfg_id} is full")
        return job_id