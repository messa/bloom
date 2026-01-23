"""
Base class for bloom filter implementations.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class BloomFilterConfig:
    """Configuration for bloom filter creation."""
    expected_items: int
    false_positive_rate: float = 0.01  # 1% default
    # Optional: some implementations allow direct size specification
    size_bits: Optional[int] = None
    num_hash_functions: Optional[int] = None


@dataclass
class BloomFilterStats:
    """Statistics about a bloom filter."""
    name: str
    size_bytes: int
    num_items: int
    expected_fpp: float
    actual_fpp: Optional[float] = None
    creation_time_ms: Optional[float] = None
    query_time_ms: Optional[float] = None


class BloomFilterImplementation(ABC):
    """Abstract base class for bloom filter implementations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this implementation."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this implementation is available on the system."""
        pass

    @abstractmethod
    def create_filter(self, config: BloomFilterConfig) -> None:
        """Create a new bloom filter with the given configuration."""
        pass

    @abstractmethod
    def add_items(self, items: list[bytes]) -> None:
        """Add items to the bloom filter."""
        pass

    @abstractmethod
    def check_items(self, items: list[bytes]) -> list[bool]:
        """Check if items might be in the filter. Returns list of booleans."""
        pass

    @abstractmethod
    def get_size_bytes(self) -> int:
        """Return the size of the filter in bytes."""
        pass

    @abstractmethod
    def save(self, path: Path) -> None:
        """Save the filter to a file."""
        pass

    @abstractmethod
    def load(self, path: Path) -> None:
        """Load a filter from a file."""
        pass

    def get_stats(self) -> BloomFilterStats:
        """Get statistics about the current filter."""
        return BloomFilterStats(
            name=self.name,
            size_bytes=self.get_size_bytes(),
            num_items=getattr(self, '_num_items', 0),
            expected_fpp=getattr(self, '_expected_fpp', 0.0),
        )
