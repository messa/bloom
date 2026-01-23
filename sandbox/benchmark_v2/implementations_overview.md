# Bloom Filter Implementations Overview

## Summary

| Implementation | Language | Library | Hash | Index Extension | Notes |
|----------------|----------|---------|------|-----------------|-------|
| trivial_python_bloom | Python | - | MD5 | .bitmap | Custom 32 MB bitmap |
| trivial_cpp_bloom | C++ | - | FNV-1a | .bitmap | Custom 32 MB bitmap |
| python_pybloomfiltermmap3 | Python/C | pybloomfiltermmap3 | internal | .bloom | mmap-backed |
| python_rbloom | Python/Rust | rbloom | MD5 (custom) | .rbloom | Fast Rust impl |
| python_blooms | Python | blooms | MD5 | .blooms | Base64 serialization |
| python_fastbloom | Python/Rust | fastbloom-rs | internal | .fastbloom | Claims 10x faster |

## Detailed Information

### trivial_python_bloom

- **File:** `trivial_python_bloom/trivial_python_bloom_ngram.py`
- **Description:** Custom implementation using a 32 MB bytearray bitmap
- **Hash:** MD5, first 8 bytes as position
- **Serialization:** Raw binary file
- **Dependencies:** None (stdlib only)

### trivial_cpp_bloom

- **File:** `trivial_cpp_bloom/trivial_cpp_bloom_ngram.cpp`
- **Description:** C++ implementation with same logic as Python trivial
- **Hash:** FNV-1a (no external crypto library needed)
- **Serialization:** Raw binary file
- **Dependencies:** liblzma, zlib
- **Build:** `make` in the directory

### python_pybloomfiltermmap3

- **File:** `python_pybloomfiltermmap3/sample_pybloomfiltermmap3_ngram.py`
- **Library:** [pybloomfiltermmap3](https://pypi.org/project/pybloomfiltermmap3/)
- **Description:** Fast implementation using memory-mapped files
- **Serialization:** Native mmap file persistence
- **Config:** 10M capacity, 1% error rate

### python_rbloom

- **File:** `python_rbloom/sample_rbloom_ngram.py`
- **Library:** [rbloom](https://github.com/KenanHanke/rbloom)
- **Description:** Rust-based, fast and lightweight
- **Hash:** Custom stable MD5-based hash (required for save/load)
- **Serialization:** Native `.save()` / `.load()` methods
- **Config:** 10M capacity, 1% error rate

### python_blooms

- **File:** `python_blooms/sample_blooms_ngram.py`
- **Library:** [blooms](https://pypi.org/project/blooms/)
- **Description:** Pure Python, derived from bytearray
- **Hash:** MD5, truncated to 3 bytes
- **Serialization:** Base64 encoded text file
- **Config:** 4 MB filter size

### python_fastbloom

- **File:** `python_fastbloom/sample_fastbloom_ngram.py`
- **Library:** [fastbloom-rs](https://pypi.org/project/fastbloom-rs/)
- **Description:** Rust implementation, claims 10x faster than pybloom
- **Serialization:** `save_to_file_with_hashes()` / `from_file_with_hashes()`
- **Config:** 10M capacity, 1% error rate
- **Note:** Library panics on missing file, explicit existence check added

## Common Configuration

All implementations use:
- **N-gram size:** 3
- **Token pattern:** `[a-zA-Z0-9:_-]+`
- **File support:** .xz (lzma), .gz (gzip), plain text

## CLI Interface

All implementations follow the same CLI pattern:

```bash
# Index a file
INDEX_DIR=<dir> ./implementation add <file>

# Check if term might exist
INDEX_DIR=<dir> ./implementation check <file> <term>
# Exit code: 0 = might contain, 1 = definitely not, 2 = error
```

## Testing

```bash
# Test single implementation
./check_implementation.py <path_to_implementation>

# Test all implementations
./check_all_implementations.sh
```
