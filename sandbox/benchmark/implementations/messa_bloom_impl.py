"""
Wrapper for messa/bloom implementation (this project).
"""

import pickle
import sys
from pathlib import Path

# Add the parent project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from .base import BloomFilterConfig, BloomFilterImplementation


class MessaBloomImplementation(BloomFilterImplementation):
    """messa/bloom implementation wrapper using the project's own bloom filter."""

    def __init__(self):
        self._array = None
        self._num_items = 0
        self._expected_fpp = 0.0
        self._array_size = 0

    @property
    def name(self) -> str:
        return "messa-bloom"

    def is_available(self) -> bool:
        try:
            from bloom.hash import fnv1a_64
            return True
        except ImportError:
            return False

    def create_filter(self, config: BloomFilterConfig) -> None:
        # Calculate array size based on expected items and FPP
        # Using formula: m = -n * ln(p) / (ln(2))^2
        import math
        n = config.expected_items
        p = config.false_positive_rate

        # Calculate optimal number of bits
        m_bits = int(-n * math.log(p) / (math.log(2) ** 2))

        # Convert to bytes, use at least 1KB
        self._array_size = max(1024, (m_bits + 7) // 8)

        # Create the byte array
        self._array = bytearray(self._array_size)
        self._expected_fpp = config.false_positive_rate
        self._num_items = 0

        # Number of hash functions: k = (m/n) * ln(2)
        self._num_hashes = max(1, int((self._array_size * 8 / n) * math.log(2)))

    def _get_hash_positions(self, item: bytes) -> list[int]:
        """Get bit positions for an item using FNV-1a hashing."""
        try:
            from bloom.hash import fnv1a_64
        except ImportError:
            from bloom._hashpy import fnv1a_64

        # Use double hashing (Kirsch-Mitzenmacher)
        h1 = fnv1a_64(item)
        h2 = fnv1a_64(item + b'\x00')  # Different input for second hash

        positions = []
        m_bits = self._array_size * 8
        for i in range(self._num_hashes):
            pos = (h1 + i * h2) % m_bits
            positions.append(pos)

        return positions

    def _set_bit(self, pos: int) -> None:
        """Set a bit at the given position."""
        byte_idx = pos // 8
        bit_idx = pos % 8
        self._array[byte_idx] |= (1 << bit_idx)

    def _get_bit(self, pos: int) -> bool:
        """Get the bit at the given position."""
        byte_idx = pos // 8
        bit_idx = pos % 8
        return bool(self._array[byte_idx] & (1 << bit_idx))

    def add_items(self, items: list[bytes]) -> None:
        if self._array is None:
            raise RuntimeError("Filter not created")

        for item in items:
            for pos in self._get_hash_positions(item):
                self._set_bit(pos)
            self._num_items += 1

    def check_items(self, items: list[bytes]) -> list[bool]:
        if self._array is None:
            raise RuntimeError("Filter not created")

        results = []
        for item in items:
            found = all(self._get_bit(pos) for pos in self._get_hash_positions(item))
            results.append(found)
        return results

    def get_size_bytes(self) -> int:
        return self._array_size if self._array is not None else 0

    def save(self, path: Path) -> None:
        if self._array is None:
            raise RuntimeError("Filter not created")

        data = {
            'array': bytes(self._array),
            'num_items': self._num_items,
            'expected_fpp': self._expected_fpp,
            'num_hashes': self._num_hashes,
        }
        with open(path, 'wb') as f:
            pickle.dump(data, f)

    def load(self, path: Path) -> None:
        with open(path, 'rb') as f:
            data = pickle.load(f)

        self._array = bytearray(data['array'])
        self._array_size = len(self._array)
        self._num_items = data['num_items']
        self._expected_fpp = data['expected_fpp']
        self._num_hashes = data['num_hashes']
