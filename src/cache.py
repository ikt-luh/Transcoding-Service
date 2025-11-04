from collections import OrderedDict
import sys

class LRUCache:
    """ A simple Least-Recently-Used (LRU) cache that evicts entries
    when the total stored size (in bytes) exceeds `max_bytes`.

    Objects must be picklable or have __sizeof__ implemented meaningfully.
    """

    def __init__(self, max_bytes=6000):
        self.max_bytes = max_bytes
        self.cache = OrderedDict()
        self.current_bytes = 0

    def _get_size(self, obj):
        """Estimate object size in bytes."""
        try:
            return sys.getsizeof(obj)
        except Exception:
            # Fallback for objects without __sizeof__
            return len(bytes(obj)) if hasattr(obj, "__bytes__") else 0

    def add(self, key, value):
        """Add or update an object in the cache."""
        size = self._get_size(value)

        # If key exists, remove old size first
        if key in self.cache:
            old_size = self._get_size(self.cache[key])
            self.current_bytes -= old_size
            del self.cache[key]

        self.cache[key] = value
        self.current_bytes += size
        self.cache.move_to_end(key)  # Mark as most recently used

        # Evict least recently used until under byte budget
        while self.current_bytes > self.max_bytes and self.cache:
            old_key, old_val = self.cache.popitem(last=False)
            self.current_bytes -= self._get_size(old_val)

    def get(self, key):
        """Retrieve an item and mark it as recently used."""
        if key not in self.cache:
            return None
        value = self.cache.pop(key)
        self.cache[key] = value  # Reinsert at end (most recently used)
        return value

    def __len__(self):
        return len(self.cache)

    def __repr__(self):
        return f"<LRUCache {len(self.cache)} items, {self.current_bytes}/{self.max_bytes} bytes>"
