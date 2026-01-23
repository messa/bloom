#!/usr/bin/env python3
"""
Parquet Split Block Bloom Filter (SBBF) implementation for benchmarking.

Based on Apache Parquet specification and arrow-rs implementation:
https://parquet.apache.org/docs/file-format/bloomfilter/
https://github.com/apache/arrow-rs/blob/main/parquet/src/bloom_filter/mod.rs

Usage:
    INDEX_DIR=indices/parquet_sbbf ./parquet_sbbf_ngram.py add foo.log.xz
    INDEX_DIR=indices/parquet_sbbf ./parquet_sbbf_ngram.py check foo.log.xz slovo
"""

import argparse
import gzip
import lzma
import os
import re
import struct
import sys
from pathlib import Path

import xxhash

# Configuration
NGRAM_SIZE = 3

# SBBF Configuration
# Target: ~32MB like trivial implementation, with 1% FPP
# For 10M distinct values at 1% FPP, we need roughly 32MB
NUM_BLOCKS = 1024 * 1024  # 1M blocks = 32 MB (each block is 32 bytes)
XXHASH_SEED = 0

# Salt values from Parquet spec
SALT = [
    0x47b6137b,
    0x44974d91,
    0x8824ad5b,
    0xa2b7289d,
    0x705495c7,
    0x2df1424b,
    0x9efc4947,
    0x5c6bfb31,
]

# Token pattern: sequence of alphanumeric chars, colons, underscores, hyphens
TOKEN_PATTERN = re.compile(r'[a-zA-Z0-9:_-]+')


def get_ngrams(token: str, n: int = NGRAM_SIZE):
    """Generate n-grams from a token string."""
    for i in range(len(token) - n + 1):
        yield token[i:i + n]


def open_file(filepath: Path):
    """Open a file, decompressing if necessary based on extension."""
    suffix = filepath.suffix.lower()
    if suffix == '.xz':
        return lzma.open(filepath, 'rt', encoding='utf-8', errors='replace')
    elif suffix == '.gz':
        return gzip.open(filepath, 'rt', encoding='utf-8', errors='replace')
    else:
        return open(filepath, 'r', encoding='utf-8', errors='replace')


class Block:
    """
    A 256-bit block consisting of 8 x 32-bit words.
    This is the fundamental unit of the Split Block Bloom Filter.
    """
    __slots__ = ('words',)

    def __init__(self):
        self.words = [0] * 8

    @staticmethod
    def mask(x: int) -> list[int]:
        """
        Generate a mask with one bit set per word.
        Uses SALT constants for mixing.
        """
        result = [0] * 8
        for i in range(8):
            # Wrapping multiply (simulate 32-bit overflow)
            y = (x * SALT[i]) & 0xFFFFFFFF
            # Shift right by 27 to get bit position (0-31)
            bit_pos = y >> 27
            result[i] = 1 << bit_pos
        return result

    def insert(self, hash_value: int):
        """Insert a hash value into the block."""
        mask = self.mask(hash_value & 0xFFFFFFFF)
        for i in range(8):
            self.words[i] |= mask[i]

    def check(self, hash_value: int) -> bool:
        """Check if a hash value might be in the block."""
        mask = self.mask(hash_value & 0xFFFFFFFF)
        for i in range(8):
            if (self.words[i] & mask[i]) == 0:
                return False
        return True

    def to_bytes(self) -> bytes:
        """Serialize block to bytes (little-endian)."""
        return struct.pack('<8I', *self.words)

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Block':
        """Deserialize block from bytes (little-endian)."""
        block = cls()
        block.words = list(struct.unpack('<8I', data))
        return block


class SplitBlockBloomFilter:
    """
    Split Block Bloom Filter (SBBF) as specified by Apache Parquet.

    Uses xxHash64 with seed=0 for hashing.
    High 32 bits of hash select the block, low 32 bits select bits within the block.
    """
    __slots__ = ('blocks', 'num_blocks')

    def __init__(self, num_blocks: int = NUM_BLOCKS):
        """Initialize SBBF with given number of blocks."""
        # Ensure num_blocks is a power of 2 for efficient modulo
        self.num_blocks = num_blocks
        self.blocks = [Block() for _ in range(num_blocks)]

    def _hash(self, value: str) -> int:
        """Hash a string value using xxHash64."""
        return xxhash.xxh64(value.encode('utf-8'), seed=XXHASH_SEED).intdigest()

    def _block_index(self, hash_value: int) -> int:
        """
        Calculate block index from hash value.
        Uses high 32 bits of hash, then maps to block range.
        """
        # Use multiplication and shift instead of modulo for better distribution
        # This matches the arrow-rs implementation
        high_bits = hash_value >> 32
        return ((high_bits * self.num_blocks) >> 32) & (self.num_blocks - 1)

    def insert(self, value: str):
        """Insert a value into the filter."""
        hash_value = self._hash(value)
        block_index = self._block_index(hash_value)
        self.blocks[block_index].insert(hash_value)

    def check(self, value: str) -> bool:
        """
        Check if a value might be in the filter.
        Returns True if the value might be present (possible false positive).
        Returns False if the value is definitely not present.
        """
        hash_value = self._hash(value)
        block_index = self._block_index(hash_value)
        return self.blocks[block_index].check(hash_value)

    def to_bytes(self) -> bytes:
        """Serialize the entire filter to bytes."""
        return b''.join(block.to_bytes() for block in self.blocks)

    @classmethod
    def from_bytes(cls, data: bytes) -> 'SplitBlockBloomFilter':
        """Deserialize filter from bytes."""
        block_size = 32  # 8 words * 4 bytes
        num_blocks = len(data) // block_size
        sbbf = cls(num_blocks)
        for i in range(num_blocks):
            block_data = data[i * block_size:(i + 1) * block_size]
            sbbf.blocks[i] = Block.from_bytes(block_data)
        return sbbf

    def size_bytes(self) -> int:
        """Return the size of the filter in bytes."""
        return self.num_blocks * 32


def add_to_index(input_file: str, index_dir: str):
    """Add a file to the bloom filter index."""
    input_path = Path(input_file)
    index_path = Path(index_dir)

    # Create index directory if it doesn't exist
    index_path.mkdir(parents=True, exist_ok=True)

    # Create SBBF
    sbbf = SplitBlockBloomFilter(NUM_BLOCKS)

    # Read file line by line, tokenize, and index n-grams
    with open_file(input_path) as f:
        for line in f:
            # Extract tokens from the line
            tokens = TOKEN_PATTERN.findall(line)
            for token in tokens:
                # Generate and index n-grams from each token
                for ngram in get_ngrams(token):
                    sbbf.insert(ngram)

    # Write SBBF to file
    sbbf_file = index_path / (input_path.name + '.sbbf')
    with open(sbbf_file, 'wb') as f:
        f.write(sbbf.to_bytes())


def check_in_index(input_file: str, index_dir: str, search_term: str) -> bool:
    """Check if a search term might be in the indexed file."""
    input_path = Path(input_file)
    index_path = Path(index_dir)

    # Read SBBF
    sbbf_file = index_path / (input_path.name + '.sbbf')
    with open(sbbf_file, 'rb') as f:
        sbbf = SplitBlockBloomFilter.from_bytes(f.read())

    # Check all n-grams of the search term
    for ngram in get_ngrams(search_term):
        if not sbbf.check(ngram):
            return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Parquet Split Block Bloom Filter (SBBF) implementation for benchmarking.'
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a file to the index')
    add_parser.add_argument('file', help='File to index')

    # Check command
    check_parser = subparsers.add_parser('check', help='Check if a term might be in an indexed file')
    check_parser.add_argument('file', help='File to check against')
    check_parser.add_argument('term', help='Search term to look for')

    args = parser.parse_args()

    index_dir = os.environ.get('INDEX_DIR')
    if not index_dir:
        print("Error: INDEX_DIR environment variable not set", file=sys.stderr)
        sys.exit(2)

    try:
        if args.command == 'add':
            add_to_index(args.file, index_dir)

        elif args.command == 'check':
            if check_in_index(args.file, index_dir, args.term):
                sys.exit(0)  # Might contain the term
            else:
                sys.exit(1)  # Definitely does not contain the term

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
