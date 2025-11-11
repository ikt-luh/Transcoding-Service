import csv
import time
import os
from multiprocessing import Queue, Process
from pathlib import Path

class CSVLogger:
    def __init__(self, path: str, fields):
        self.path = Path(path)
        self.fields = fields
        self.queue = Queue()

        # ensure directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # write header once
        self.proc = Process(target=self._writer, daemon=True)
        self.proc.start()
        if not self.path.exists():
            with open(self.path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fields)
                writer.writeheader()

    def _writer(self):
        """Runs in a separate process. Writes CSV rows from the queue."""
        write_header = not self.path.exists()
        with open(self.path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            if write_header:
                writer.writeheader()

            while True:
                row = self.queue.get()
                if row is None:
                    break  # termination signal
                writer.writerow(row)
                f.flush()  # low overhead, ensures safety

    def log(self, **kwargs):
        row = {f: kwargs.get(f, None) for f in self.fields}
        self.queue.put(row)

    def close(self):
        """Flush + terminate writer."""
        self.queue.put(None)
        self.proc.join()