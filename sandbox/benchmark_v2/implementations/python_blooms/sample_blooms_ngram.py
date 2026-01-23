#!/usr/bin/env python3
"""
Bloom filter implementation using blooms library.

Usage:
    INDEX_DIR=indices/blooms ./sample_blooms_ngram.py add foo.log.xz
    INDEX_DIR=indices/blooms ./sample_blooms_ngram.py check foo.log.xz slovo
"""

import argparse
import gzip
import hashlib
import lzma
import os
import re
import sys
from pathlib import Path

from blooms import blooms

# Configuration
BLOOM_SIZE = 4 * 1024 * 1024  # 4 MB (32 million bits)
HASH_TRUNCATE = 3  # Use 3 bytes of hash (24 bits for addressing)
NGRAM_SIZE = 3

# Token pattern: sequence of alphanumeric chars, colons, underscores, hyphens
TOKEN_PATTERN = re.compile(r'[a-zA-Z0-9:_-]+')


def hash_ngram(ngram: str) -> bytes:
    """Hash an n-gram and truncate for bloom filter."""
    return hashlib.md5(ngram.encode('utf-8')).digest()[:HASH_TRUNCATE]


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


def add_to_index(input_file: str, index_dir: str):
    """Add a file to the bloom filter index."""
    input_path = Path(input_file)
    index_path = Path(index_dir)

    # Create index directory if it doesn't exist
    index_path.mkdir(parents=True, exist_ok=True)

    # Create bloom filter
    bf = blooms(BLOOM_SIZE)

    # Read file line by line, tokenize, and index n-grams
    with open_file(input_path) as f:
        for line in f:
            # Extract tokens from the line
            tokens = TOKEN_PATTERN.findall(line)
            for token in tokens:
                # Generate and index n-grams from each token
                for ngram in get_ngrams(token):
                    bf @= hash_ngram(ngram)

    # Save bloom filter to file using base64
    bloom_file = index_path / (input_path.name + '.blooms')
    with open(bloom_file, 'w') as f:
        f.write(bf.to_base64())


def check_in_index(input_file: str, index_dir: str, search_term: str) -> bool:
    """Check if a search term might be in the indexed file."""
    input_path = Path(input_file)
    index_path = Path(index_dir)

    # Load existing bloom filter
    bloom_file = index_path / (input_path.name + '.blooms')
    with open(bloom_file, 'r') as f:
        bf = blooms.from_base64(f.read())

    # Check all n-grams of the search term
    for ngram in get_ngrams(search_term):
        if not (hash_ngram(ngram) @ bf):
            return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Bloom filter implementation using blooms library.'
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
