from collections import OrderedDict
from threading import RLock
import os
import time
import sys

class LRUCache:
    """
    Thread-safe LRU cache with max total byte size.
    """

    def __init__(self, max_bytes, logger=None):
        self.max_bytes = max_bytes
        self.cache = OrderedDict()
        self.current_bytes = 0
        self.logger = logger
        self._lock = RLock()   # single global lock

    def _get_size(self, obj):
        """
        Return size of cached object in bytes.
        For file paths, use file size; otherwise fallback to object __sizeof__().
        Always returns int.
        """
        if isinstance(obj, str) and os.path.exists(obj):
            try:
                return os.path.getsize(obj)
            except OSError:
                pass
        return sys.getsizeof(obj)

    def _log(self, event, key):
        if self.logger is not None:
            self.logger.log(
                timestamp=time.time(),
                event=event,
                key=key,
                cache_items=len(self.cache),
                cache_bytes=self.current_bytes,
            )

    def add(self, key, value):
        if self.max_bytes <= 0:
            return None

        with self._lock:
            size = self._get_size(value)

            # Remove existing entry size first
            if key in self.cache:
                self.current_bytes -= self._get_size(self.cache[key])
                self.cache.pop(key)

            self.cache[key] = value
            self.current_bytes += size
            self.cache.move_to_end(key)
            self._log("cache_add", key)

            # Evict until under limit
            while self.current_bytes > self.max_bytes and self.cache:
                old_key, old_val = self.cache.popitem(last=False)
                self.current_bytes -= self._get_size(old_val)
                self._log("cache_evict", old_key)

    def get(self, key):
        if self.max_bytes <= 0:
            return None

        with self._lock:
            if key not in self.cache:
                self._log("cache_miss", key)
                return None

            val = self.cache.pop(key)
            self.cache[key] = val
            self._log("cache_hit", key)
            return val

    def __len__(self):
        with self._lock:
            return len(self.cache)

    def __repr__(self):
        with self._lock:
            return f"<LRUCache {len(self.cache)} items, {self.current_bytes}/{self.max_bytes} bytes>"
