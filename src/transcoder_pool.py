import time
import os
import uuid
import threading
import multiprocessing
from queue import Empty

from rabbit import Transcoder, TranscoderConfig

def _worker_loop_dynamic(gpu_id: int, queue: multiprocessing.Queue, configs: dict, done_queue):
    """Worker that can switch config per job (dynamic mode)."""
    try:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)
        transcoder = Transcoder()  
    except Exception as e:
        print(f"[Worker][GPU {gpu_id}] Failed to init transcoder: {e}", flush=True)
        return
    
    while True:
        job = queue.get()
        if job is None:
            print(f"[Pool][Worker][GPU {gpu_id}] shutting down.")
            break

        job_id, cfg_id, src_path, out_path = job
        cfg_dict = configs[cfg_id]

        transcoder.set_config(
            TranscoderConfig(
                use_cuda=cfg_dict.get("cuda", False),
                geometry_qp=cfg_dict.get("geoQP", 32),
                attribute_qp=cfg_dict.get("attQP", 32),
                preset=cfg_dict.get("preset", "ultrafast"),
                gpuID=0,
            )
        )

        try:
            print(f"[Pool][GPU {gpu_id}] ({cfg_id}) Transcoding {src_path} -> {out_path}")
            transcoder.transcode(src_path, out_path)
            done_queue.put(job_id)
        except Exception as e:
            print(f"[Pool][GPU {gpu_id}] Error on job {job_id}: {e}")
            done_queue.put(job_id)

def _worker_loop_per_config(cfg_id: str, cfg_dict: dict, gpu_id: int, queue: multiprocessing.Queue, done_queue):
    """Worker loop: run transcoder jobs for one configuration on one GPU."""
    try:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

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
            done_queue.put(job_id)
        except Exception as e:
            print(f"[Worker-{cfg_id}][GPU {gpu_id}] Error on job {job_id}: {e}", flush=True)
            done_queue.put(job_id)


class TranscoderPool:
    """Multi-GPU transcoder pool managing per-configuration queues and workers."""

    def __init__(self, gpu_plan: dict[int, int], configs: dict[str, dict], mode="per_config"):
        self.gpu_plan = gpu_plan or {0: 1}
        self.configs = configs

        self.done_queue = multiprocessing.Queue()
        # Dynamic or static pool mode
        self.mode = mode
        assert mode in ("per_config", "dynamic")
        if self.mode == "per_config":
            self.queues = {cfg_id: multiprocessing.Queue(maxsize=100) for cfg_id in configs}
            self.procs = {cfg_id: [] for cfg_id in configs}
        else:
            self.queue = multiprocessing.Queue(maxsize=400)
            self.procs = []

        self.lock = threading.Lock()

    def start(self):
        total_workers = sum(self.gpu_plan.values())
        print(f"[Pool] Starting {total_workers} workers, mode={self.mode}")

        gpu_slots = []
        for gpu_id, cap in self.gpu_plan.items():
            gpu_slots.extend([gpu_id] * cap)

        if self.mode == "dynamic":
            self._start_dynamic(gpu_slots)
        else: 
            self._start_per_config(gpu_slots, total_workers)

    def _start_dynamic(self, gpu_slots):
        for i, gpu_id in enumerate(gpu_slots):
            p = multiprocessing.Process(
                target=_worker_loop_dynamic,
                args=(gpu_id, self.queue, self.configs, self.done_queue),
            )
            p.start()
            self.procs.append(p)
            print(f"[Pool] Started dynamic worker {i} on GPU {gpu_id}")

    def _start_per_config(self, gpu_slots, total_workers):
        cfg_ids = list(self.configs.keys())
        base = total_workers // len(cfg_ids)
        remainder = total_workers % len(cfg_ids)
        idx = 0

        for i, cfg_id in enumerate(cfg_ids):
            n = base + (1 if i < remainder else 0)
            for _ in range(n):
                gpu_id = gpu_slots[idx % len(gpu_slots)]
                p = multiprocessing.Process(
                    target=_worker_loop_per_config,
                    args=(cfg_id, self.configs[cfg_id], gpu_id, self.queues[cfg_id], self.done_queue),
                )
                p.start()
                self.procs[cfg_id].append(p)
                idx += 1
                print(f"[Pool] Started per-config worker {i} with config {cfg_id} on GPU {gpu_id}")

    def stop(self):
        """ Shutdown all workers. """
        # Shutdown is done by filling queues with None and joining procs.
        if self.mode == "dynamic":
            for _ in range(len(self.procs)):
                self.queue.put(None)
            for p in self.procs:
                p.join()
        else:
            for cfg_id, q in self.queues.items():
                for _ in self.procs[cfg_id]:
                    q.put(None)
            for plist in self.procs.values():
                for p in plist:
                    p.join()

        print("[Pool] Shutdown complete.")


    def submit(self, cfg_id: str, src_path: str, out_path: str):
        job_id = str(uuid.uuid4())
        with self.lock:
            if self.mode == "dynamic":
                self.queue.put_nowait((job_id, cfg_id, src_path, out_path))
                print(f"[Pool] Submitted Job {job_id} with dynamic queue ({cfg_id})")
            else:
                self.queues[cfg_id].put_nowait((job_id, src_path, out_path))
                print(f"[Pool] Submitted Job {job_id} with cfg {cfg_id}")
        return job_id


    def wait_all(self, job_ids):
        remaining = set(job_ids)
        while remaining:
            job_id = self.done_queue.get()  # blocks
            remaining.discard(job_id)
