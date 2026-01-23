#!/usr/bin/env python3
"""
Trivial Bloom filter implementation for benchmarking.

Usage:
    INDEX_DIR=indices/trivial_python_bloom ./trivial_python_bloom.py add foo.log.xz
    INDEX_DIR=indices/trivial_python_bloom ./trivial_python_bloom.py check foo.log.xz slovo
"""

import hashlib
import lzma
import os
import sys
from pathlib import Path

# Configuration
BITMAP_SIZE = 32 * 1024 * 1024  # 32 MB
BITMAP_BITS = BITMAP_SIZE * 8
NGRAM_SIZE = 3
NUM_HASHES = 3


def get_ngrams(data: bytes, n: int = NGRAM_SIZE):
    """Generate n-grams from bytes data."""
    for i in range(len(data) - n + 1):
        yield data[i:i + n]


def hash_ngram(ngram: bytes, seed: int) -> int:
    """Hash an n-gram to a bit position."""
    h = hashlib.md5(bytes([seed]) + ngram).digest()
    # Use first 8 bytes as integer
    value = int.from_bytes(h[:8], 'little')
    return value % BITMAP_BITS


def set_bit(bitmap: bytearray, bit_pos: int):
    """Set a bit in the bitmap."""
    byte_pos = bit_pos // 8
    bit_offset = bit_pos % 8
    bitmap[byte_pos] |= (1 << bit_offset)


def get_bit(bitmap: bytes, bit_pos: int) -> bool:
    """Get a bit from the bitmap."""
    byte_pos = bit_pos // 8
    bit_offset = bit_pos % 8
    return bool(bitmap[byte_pos] & (1 << bit_offset))


def add_to_index(input_file: str, index_dir: str):
    """Add a file to the bloom filter index."""
    input_path = Path(input_file)
    index_path = Path(index_dir)

    # Create index directory if it doesn't exist
    index_path.mkdir(parents=True, exist_ok=True)

    # Read and decompress the file
    with lzma.open(input_path, 'rb') as f:
        content = f.read()

    # Create empty bitmap
    bitmap = bytearray(BITMAP_SIZE)

    # Add all n-grams to the bloom filter
    for ngram in get_ngrams(content):
        for seed in range(NUM_HASHES):
            bit_pos = hash_ngram(ngram, seed)
            set_bit(bitmap, bit_pos)

    # Write bitmap to file
    bitmap_file = index_path / (input_path.name + '.bitmap')
    with open(bitmap_file, 'wb') as f:
        f.write(bitmap)


def check_in_index(input_file: str, index_dir: str, search_term: str) -> bool:
    """Check if a search term might be in the indexed file."""
    input_path = Path(input_file)
    index_path = Path(index_dir)

    # Read bitmap
    bitmap_file = index_path / (input_path.name + '.bitmap')
    with open(bitmap_file, 'rb') as f:
        bitmap = f.read()

    # Convert search term to bytes
    search_bytes = search_term.encode('utf-8')

    # Check all n-grams of the search term
    for ngram in get_ngrams(search_bytes):
        for seed in range(NUM_HASHES):
            bit_pos = hash_ngram(ngram, seed)
            if not get_bit(bitmap, bit_pos):
                return False

    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: trivial_python_bloom.py <add|check> ...", file=sys.stderr)
        sys.exit(2)

    command = sys.argv[1]
    index_dir = os.environ.get('INDEX_DIR')

    if not index_dir:
        print("Error: INDEX_DIR environment variable not set", file=sys.stderr)
        sys.exit(2)

    try:
        if command == 'add':
            if len(sys.argv) != 3:
                print("Usage: trivial_python_bloom.py add <file>", file=sys.stderr)
                sys.exit(2)
            input_file = sys.argv[2]
            add_to_index(input_file, index_dir)

        elif command == 'check':
            if len(sys.argv) != 4:
                print("Usage: trivial_python_bloom.py check <file> <search_term>", file=sys.stderr)
                sys.exit(2)
            input_file = sys.argv[2]
            search_term = sys.argv[3]

            if check_in_index(input_file, index_dir, search_term):
                sys.exit(0)  # Might contain the term
            else:
                sys.exit(1)  # Definitely does not contain the term

        else:
            print(f"Unknown command: {command}", file=sys.stderr)
            sys.exit(2)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
