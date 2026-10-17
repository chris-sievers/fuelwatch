"""In-process response cache.

Used only from the event loop, so it needs no lock. Each entry keeps the
value, the time it was stored and a tag; a lookup misses when the entry is
older than its TTL or its tag differs (the summary is tagged with the
scheduler's collection count, so it is cached until the next collection).
Size is bounded: the oldest entries go first, so varied query strings
cannot grow memory without limit.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Hashable

SERIES_TTL = 60.0  # /api/series and /api/data, seconds
SUMMARY_MAX_AGE = 3600.0  # /api/summary is also refreshed at least this often
MAX_ENTRIES = 512


class Cache:
    def __init__(self, clock: Callable[[], float], max_entries: int = MAX_ENTRIES) -> None:
        self._clock = clock
        self._max = max_entries
        self._items: OrderedDict[Hashable, tuple[float, object, object]] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: Hashable, ttl: float, tag: object = None):
        """The cached value, or None on a miss."""
        item = self._items.get(key)
        if item is not None:
            stored, item_tag, value = item
            age = self._clock() - stored
            if 0 <= age < ttl and item_tag == tag:
                self._items.move_to_end(key)
                self.hits += 1
                return value
            del self._items[key]
        self.misses += 1
        return None

    def put(self, key: Hashable, value: object, tag: object = None) -> None:
        self._items[key] = (self._clock(), tag, value)
        self._items.move_to_end(key)
        while len(self._items) > self._max:
            self._items.popitem(last=False)

    def clear(self) -> None:
        self._items.clear()

    def __len__(self) -> int:
        return len(self._items)
