"""
Wrapper for fastbloom-rs bloom filter implementation.
https://github.com/yankun1992/fastbloom
"""

import pickle
from pathlib import Path
from typing import Optional

from .base import BloomFilterConfig, BloomFilterImplementation


class FastBloomImplementation(BloomFilterImplementation):
    """fastbloom-rs implementation wrapper."""

    def __init__(self):
        self._filter = None
        self._num_items = 0
        self._expected_fpp = 0.0
        self._size_bytes = 0

    @property
    def name(self) -> str:
        return "fastbloom-rs"

    def is_available(self) -> bool:
        try:
            from fastbloom_rs import BloomFilter
            return True
        except ImportError:
            return False

    def create_filter(self, config: BloomFilterConfig) -> None:
        from fastbloom_rs import BloomFilter

        self._filter = BloomFilter(
            config.expected_items,
            config.false_positive_rate
        )
        self._expected_fpp = config.false_positive_rate
        self._num_items = 0
        self._update_size()

    def add_items(self, items: list[bytes]) -> None:
        if self._filter is None:
            raise RuntimeError("Filter not created")

        for item in items:
            self._filter.add(item)
            self._num_items += 1

        self._update_size()

    def check_items(self, items: list[bytes]) -> list[bool]:
        if self._filter is None:
            raise RuntimeError("Filter not created")

        return [self._filter.contains(item) for item in items]

    def get_size_bytes(self) -> int:
        return self._size_bytes

    def _update_size(self):
        if self._filter is not None:
            # fastbloom uses get_bytes() to get the raw bytes
            try:
                self._size_bytes = len(self._filter.get_bytes())
            except AttributeError:
                # Estimate from config if get_bytes not available
                self._size_bytes = self._filter.hashes() * 8  # rough estimate

    def save(self, path: Path) -> None:
        if self._filter is None:
            raise RuntimeError("Filter not created")

        data = {
            'bytes': self._filter.get_bytes(),
            'hashes': self._filter.hashes(),
            'num_items': self._num_items,
            'expected_fpp': self._expected_fpp,
        }
        with open(path, 'wb') as f:
            pickle.dump(data, f)

    def load(self, path: Path) -> None:
        from fastbloom_rs import BloomFilter

        with open(path, 'rb') as f:
            data = pickle.load(f)

        self._filter = BloomFilter.from_bytes(data['bytes'], data['hashes'])
        self._num_items = data['num_items']
        self._expected_fpp = data['expected_fpp']
        self._update_size()
