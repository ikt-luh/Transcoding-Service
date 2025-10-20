import uuid
import threading
import multiprocessing 

from transcoder import Transcoder # Note this is a relative import from the base container

def _worker_loop(pool):
    print("WORKER LOOP INIT")
    transcoder = Transcoder(None)

    while True:
        item = pool.in_queue.get()
        if item is None:
            break
        job_id, src_path, out_path, config = item

        try:
            print(f"in: {src_path} out: {out_path}", flush=True)
            transcoder.transcode(src_path, out_path, config)
            pool.mark_done(job_id)
        except Exception as e:
            print(f"[Decoder Worker] Error: {e}", flush=True)


class TranscoderPool():
    def __init__(self, num_workers):   
        self.num_workers = num_workers
        self.in_queue = multiprocessing.Queue(100)
        self.procs = []
        self.queued_jobs = set()
        self.lock = threading.Lock()


    def start(self):
        self.running = True
        for _ in range(self.num_workers):
            p = multiprocessing.Process(
                target=_worker_loop,
                args=(self,),
            )
            p.start()
            self.procs.append(p)

    def stop(self):
        for _ in self.procs:
            self.in_queue.put(None)
        for p in self.procs:
            p.join()

    def submit(self, src_path, out_path, config):
        job_id = str(uuid.uuid4())
        print(job_id)
        with self.lock:
            if job_id in self.queued_jobs: # Make a actual look up based on sequence + config + segment
                return False
            try:
                self.in_queue.put_nowait((job_id, src_path, out_path, config))
                self.queued_jobs.add(job_id)
            except multiprocessing.queues.Full:
                raise ValueError("Transcoding queue full")
        return job_id

    def mark_done(self, job_id):
        with self.lock:
            self.queued_jobs.discard(job_id)