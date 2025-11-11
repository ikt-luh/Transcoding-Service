import time
import os
import uuid
import threading
from queue import Queue
from multiprocessing import Process, Queue as MPQueue

from rabbit import Transcoder, TranscoderConfig, BitstreamIO


def _worker_process(gpu_id, worker_id, config, job_queue: MPQueue, done_queue: MPQueue, log_queue: MPQueue):
    """
    Each worker is its own process.
    Internally pipelined:
    PCCContext never leaves this process, only file paths do.
    """
    os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

    default_config = config["1"]
    transcoder = Transcoder(
        TranscoderConfig(
            use_cuda=default_config.get("cuda", True),
            geometry_qp=default_config.get("geoQP", 32),
            attribute_qp=default_config.get("attQP", 32),
            preset=default_config.get("preset", "p2"),
            gpuID=gpu_id,
        )
    )

    bitio = BitstreamIO()

    q_decode = Queue(maxsize=3)
    q_gpu = Queue(maxsize=3)

    def log(event, job_id=None):
        log_queue.put({
            "timestamp": time.time(),
            "worker_gpu": gpu_id,
            "worker_id": worker_id,
            "event": event,
            "job_id": job_id,
            "q_decode": q_decode.qsize(),
            "q_gpu": q_gpu.qsize(),
        })

    def reader():
        while True:
            item = job_queue.get()
            if item is None:
                q_decode.put(None)
                return

            job_id, src_path, out_path, config_id = item
            log("reader_start", job_id)

            # CPU decode → PCCContexts
            contexts = bitio.read(src_path)
            log("reader_end", job_id)
            q_decode.put((job_id, contexts, out_path, config_id))

    def gpu():
        while True:
            item = q_decode.get()
            if item is None:
                q_gpu.put(None)
                return

            job_id, contexts, out_path, config_id = item
            log("gpu_start", job_id)

            # Apply override config if given
            cfg_dict = config[config_id]
            transcoder.set_config(
                TranscoderConfig(
                    use_cuda=cfg_dict.get("cuda", cfg_dict.get("cuda", False)),
                    geometry_qp=cfg_dict.get("geoQP", cfg_dict.get("geoQP", 32)),
                    attribute_qp=cfg_dict.get("attQP", cfg_dict.get("attQP", 32)),
                    preset=cfg_dict.get("preset", cfg_dict.get("preset", "ultrafast")),
                    gpuID=gpu_id,
                )
                )

            # GPU transcode stage
            transcoder.transcode_contexts(contexts)

            log("gpu_end", job_id)
            q_gpu.put((job_id, contexts, out_path))

    def writer():
        while True:
            item = q_gpu.get()
            if item is None:
                return

            job_id, contexts, out_path = item
            log("writer_start", job_id)

            # CPU write stage
            bitio.write(contexts, out_path)

            log("writer_end", job_id)

            done_queue.put(job_id)

    threads = [
        threading.Thread(target=reader, daemon=True),
        threading.Thread(target=gpu, daemon=True),
        threading.Thread(target=writer, daemon=True),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()


class TranscoderPool:
    """
    Process pool with in-process decode→GPU→encode pipeline.
    No PCCContext ever crosses process boundaries.
    """

    def __init__(self, gpu_plan: dict[int, int], configs, logger):
        self.gpu_plan = gpu_plan or {0: 1}
        self.configs = configs
        self.threshold = 30

        self.job_queue = MPQueue()
        self.done_queue = MPQueue()
        self.processes = []

        self.logger = logger
        self.log_queue = logger.queue  # <-- use logger's MP-safe queue

    def start(self):
        for gpu_id, n_workers in self.gpu_plan.items():
            for worker_id in range(n_workers):
                p = Process(
                    target=_worker_process,
                    args=(gpu_id, worker_id, self.configs, self.job_queue, self.done_queue, self.log_queue),
                    daemon=True,
                )
                p.start()
                self.processes.append(p)
                print(f"[Pool] Worker started ID={worker_id}, gpu={gpu_id}")

    def stop(self):
        for _ in self.processes:
            self.job_queue.put(None)
        for p in self.processes:
            p.join()
        print("[Pool] Shutdown complete.")

    def submit(self, cfg_id: str, src_path: str, out_path: str):
        job_id = uuid.uuid4().hex
        self.job_queue.put((job_id, src_path, out_path, cfg_id))
        self.log_queue.put({
            "timestamp": time.time(),
            "worker_gpu": None,
            "worker_id": None,
            "event": "submitted",
            "job_id": job_id,
            "q_decode": None,
            "q_gpu": None,
        })
        return job_id

    def get_done(self, timeout=None):
        return self.done_queue.get(timeout=timeout)

    def queue_full(self):
        return self.job_queue.qsize() >= self.threshold

    def current_queue_length(self):
        return self.job_queue.qsize()

    def wait_all(self, job_ids, timeout=None):
        """
        Block until all job_ids have completed.
        """
        pending = set(job_ids)
        start = time.time()

        while pending:
            try:
                job_id = self.done_queue.get(timeout=0.1)
                pending.discard(job_id)
            except Exception:
                pass

            if timeout and (time.time() - start) > timeout:
                return False

        return True