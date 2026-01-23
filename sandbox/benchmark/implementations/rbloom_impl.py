"""
Wrapper for rbloom bloom filter implementation.
https://github.com/KenanHanke/rbloom
"""

import pickle
from pathlib import Path

from .base import BloomFilterConfig, BloomFilterImplementation


class RBloomImplementation(BloomFilterImplementation):
    """rbloom implementation wrapper."""

    def __init__(self):
        self._filter = None
        self._num_items = 0
        self._expected_fpp = 0.0
        self._expected_items = 0

    @property
    def name(self) -> str:
        return "rbloom"

    def is_available(self) -> bool:
        try:
            from rbloom import Bloom
            return True
        except ImportError:
            return False

    def create_filter(self, config: BloomFilterConfig) -> None:
        from rbloom import Bloom

        self._filter = Bloom(config.expected_items, config.false_positive_rate)
        self._expected_fpp = config.false_positive_rate
        self._expected_items = config.expected_items
        self._num_items = 0

    def add_items(self, items: list[bytes]) -> None:
        if self._filter is None:
            raise RuntimeError("Filter not created")

        for item in items:
            self._filter.add(item)
            self._num_items += 1

    def check_items(self, items: list[bytes]) -> list[bool]:
        if self._filter is None:
            raise RuntimeError("Filter not created")

        return [item in self._filter for item in items]

    def get_size_bytes(self) -> int:
        if self._filter is None:
            return 0
        # rbloom doesn't expose size directly when using built-in hash
        # Estimate: ~1.44 * n * log2(1/p) bits (optimal bloom filter formula)
        import math
        bits = int(1.44 * self._expected_items * math.log2(1 / self._expected_fpp))
        return (bits + 7) // 8

    def save(self, path: Path) -> None:
        if self._filter is None:
            raise RuntimeError("Filter not created")

        data = {
            'filter_bytes': self._filter.save_bytes(),
            'num_items': self._num_items,
            'expected_fpp': self._expected_fpp,
            'expected_items': self._expected_items,
        }
        with open(path, 'wb') as f:
            pickle.dump(data, f)

    def load(self, path: Path) -> None:
        from rbloom import Bloom

        with open(path, 'rb') as f:
            data = pickle.load(f)

        self._filter = Bloom.load_bytes(
            data['filter_bytes'],
            data['expected_items'],
            data['expected_fpp']
        )
        self._num_items = data['num_items']
        self._expected_fpp = data['expected_fpp']
        self._expected_items = data['expected_items']
