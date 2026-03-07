"""
Bloom filter implementations for benchmark.
"""

from .base import BloomFilterConfig, BloomFilterImplementation, BloomFilterStats


def get_all_implementations() -> list[BloomFilterImplementation]:
    """Get all available bloom filter implementations."""
    implementations = []

    # Python-based implementations
    try:
        from .fastbloom_impl import FastBloomImplementation
        implementations.append(FastBloomImplementation())
    except ImportError:
        pass

    try:
        from .rbloom_impl import RBloomImplementation
        implementations.append(RBloomImplementation())
    except ImportError:
        pass

    try:
        from .messa_bloom_impl import MessaBloomImplementation
        implementations.append(MessaBloomImplementation())
    except ImportError:
        pass

    # Go-based implementations (subprocess)
    try:
        from .bits_and_blooms_impl import BitsAndBloomsImplementation
        implementations.append(BitsAndBloomsImplementation())
    except ImportError:
        pass

    try:
        from .dcso_bloom_impl import DCSOBloomImplementation
        implementations.append(DCSOBloomImplementation())
    except ImportError:
        pass

    try:
        from .pagebloomfilter_impl import PageBloomFilterImplementation
        implementations.append(PageBloomFilterImplementation())
    except ImportError:
        pass

    return implementations


def get_available_implementations() -> list[BloomFilterImplementation]:
    """Get only implementations that are available on this system."""
    return [impl for impl in get_all_implementations() if impl.is_available()]


__all__ = [
    'BloomFilterConfig',
    'BloomFilterImplementation',
    'BloomFilterStats',
    'get_all_implementations',
    'get_available_implementations',
]
