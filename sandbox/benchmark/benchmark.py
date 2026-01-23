#!/usr/bin/env python3
"""
Bloom filter benchmark program.

This benchmark evaluates different bloom filter implementations on:
1. False positive rate (FPP)
2. Index size relative to compressed log size
3. Index creation time
4. Query time
"""

import argparse
import gzip
import json
import lzma
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from implementations import (
    BloomFilterConfig,
    get_all_implementations,
    get_available_implementations,
)


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark run."""
    logs_dir: Path
    output_dir: Path
    target_fpp: float = 0.01  # 1% default
    sample_sizes: list[int] = field(default_factory=lambda: [4, 5, 6])
    num_queries: int = 10000
    random_seed: int = 42


@dataclass
class BenchmarkResult:
    """Result of a benchmark run for one implementation."""
    implementation_name: str
    log_file: str
    compressed_size_bytes: int
    uncompressed_size_bytes: int
    index_size_bytes: int
    index_size_ratio: float
    num_items_indexed: int
    num_queries: int
    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int  # Should always be 0 for bloom filters
    actual_fpp: float
    target_fpp: float
    index_creation_time_ms: float
    query_time_ms: float


def read_compressed_file(path: Path) -> tuple[bytes, int]:
    """Read a compressed file and return (content, compressed_size)."""
    compressed_size = path.stat().st_size

    if path.suffix == '.xz':
        with lzma.open(path, 'rb') as f:
            content = f.read()
    elif path.suffix == '.gz':
        with gzip.open(path, 'rb') as f:
            content = f.read()
    else:
        with open(path, 'rb') as f:
            content = f.read()

    return content, compressed_size


def extract_samples(content: bytes, sample_sizes: list[int]) -> set[bytes]:
    """Extract n-gram samples from content."""
    samples = set()
    lines = content.split(b'\n')

    for line in lines:
        line = line.lower()  # Normalize to lowercase
        for size in sample_sizes:
            if len(line) >= size:
                for i in range(len(line) - size + 1):
                    samples.add(line[i:i + size])

    return samples


def generate_queries(
    content: bytes,
    sample_sizes: list[int],
    num_queries: int,
    rng: random.Random
) -> tuple[list[bytes], set[bytes]]:
    """
    Generate test queries.

    Returns:
        (queries, true_samples) - queries to test and set of samples actually in content
    """
    # Extract all samples from content
    true_samples = extract_samples(content, sample_sizes)

    queries = []

    # 50% of queries should be actual samples from the content
    actual_samples = list(true_samples)
    num_true_queries = min(num_queries // 2, len(actual_samples))
    queries.extend(rng.sample(actual_samples, num_true_queries))

    # 50% of queries should be random strings (likely not in content)
    num_false_queries = num_queries - num_true_queries
    for _ in range(num_false_queries):
        # Generate random bytes of random sample size
        size = rng.choice(sample_sizes)
        random_query = bytes(rng.randint(0, 255) for _ in range(size))
        queries.append(random_query)

    rng.shuffle(queries)
    return queries, true_samples


def run_benchmark_for_implementation(
    impl,
    log_path: Path,
    config: BenchmarkConfig,
    rng: random.Random,
) -> BenchmarkResult:
    """Run benchmark for a single implementation on a single log file."""
    print(f"  Benchmarking {impl.name} on {log_path.name}...")

    # Read the log file
    content, compressed_size = read_compressed_file(log_path)
    uncompressed_size = len(content)

    # Extract samples
    samples = extract_samples(content, config.sample_sizes)
    samples_list = list(samples)

    # Estimate number of items for filter sizing
    expected_items = len(samples_list)

    # Create filter
    start_time = time.perf_counter()
    filter_config = BloomFilterConfig(
        expected_items=expected_items,
        false_positive_rate=config.target_fpp
    )
    impl.create_filter(filter_config)

    # Add items in batches
    batch_size = 10000
    for i in range(0, len(samples_list), batch_size):
        batch = samples_list[i:i + batch_size]
        impl.add_items(batch)

    creation_time_ms = (time.perf_counter() - start_time) * 1000
    index_size = impl.get_size_bytes()

    # Generate queries
    queries, true_samples = generate_queries(
        content, config.sample_sizes, config.num_queries, rng
    )

    # Run queries
    start_time = time.perf_counter()
    results = impl.check_items(queries)
    query_time_ms = (time.perf_counter() - start_time) * 1000

    # Analyze results
    true_positives = 0
    false_positives = 0
    true_negatives = 0
    false_negatives = 0

    for query, found in zip(queries, results):
        actually_present = query in true_samples
        if found:
            if actually_present:
                true_positives += 1
            else:
                false_positives += 1
        else:
            if actually_present:
                false_negatives += 1
            else:
                true_negatives += 1

    # Calculate actual FPP
    total_negatives = false_positives + true_negatives
    actual_fpp = false_positives / total_negatives if total_negatives > 0 else 0.0

    return BenchmarkResult(
        implementation_name=impl.name,
        log_file=log_path.name,
        compressed_size_bytes=compressed_size,
        uncompressed_size_bytes=uncompressed_size,
        index_size_bytes=index_size,
        index_size_ratio=index_size / compressed_size if compressed_size > 0 else 0,
        num_items_indexed=len(samples_list),
        num_queries=len(queries),
        true_positives=true_positives,
        false_positives=false_positives,
        true_negatives=true_negatives,
        false_negatives=false_negatives,
        actual_fpp=actual_fpp,
        target_fpp=config.target_fpp,
        index_creation_time_ms=creation_time_ms,
        query_time_ms=query_time_ms,
    )


def run_benchmark(config: BenchmarkConfig) -> list[BenchmarkResult]:
    """Run the full benchmark suite."""
    rng = random.Random(config.random_seed)

    # Find available implementations
    print("Checking available implementations...")
    all_impls = get_all_implementations()
    available_impls = get_available_implementations()

    print(f"Found {len(all_impls)} implementations, {len(available_impls)} available:")
    for impl in all_impls:
        status = "OK" if impl in available_impls else "NOT AVAILABLE"
        print(f"  - {impl.name}: {status}")
    print()

    if not available_impls:
        print("ERROR: No implementations available!")
        return []

    # Find log files
    log_files = list(config.logs_dir.glob("*.xz")) + list(config.logs_dir.glob("*.gz"))
    if not log_files:
        print(f"ERROR: No log files found in {config.logs_dir}")
        return []

    print(f"Found {len(log_files)} log files")
    print()

    results = []

    for log_path in sorted(log_files):
        print(f"Processing {log_path.name}...")

        for impl in available_impls:
            try:
                result = run_benchmark_for_implementation(
                    impl, log_path, config, rng
                )
                results.append(result)
                print(f"    FPP: {result.actual_fpp:.4f} (target: {result.target_fpp:.4f})")
                print(f"    Index ratio: {result.index_size_ratio:.4f}")
            except Exception as e:
                print(f"    ERROR: {e}")

        print()

    return results


def print_summary(results: list[BenchmarkResult]):
    """Print a summary of benchmark results."""
    if not results:
        print("No results to summarize.")
        return

    print("=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)
    print()

    # Group by implementation
    by_impl = {}
    for r in results:
        if r.implementation_name not in by_impl:
            by_impl[r.implementation_name] = []
        by_impl[r.implementation_name].append(r)

    print(f"{'Implementation':<20} {'Avg FPP':>10} {'Max FPP':>10} {'Avg Ratio':>12} {'Max Ratio':>12}")
    print("-" * 70)

    for impl_name, impl_results in sorted(by_impl.items()):
        avg_fpp = sum(r.actual_fpp for r in impl_results) / len(impl_results)
        max_fpp = max(r.actual_fpp for r in impl_results)
        avg_ratio = sum(r.index_size_ratio for r in impl_results) / len(impl_results)
        max_ratio = max(r.index_size_ratio for r in impl_results)

        print(f"{impl_name:<20} {avg_fpp:>10.4f} {max_fpp:>10.4f} {avg_ratio:>12.4f} {max_ratio:>12.4f}")

    print()
    print("Target: FPP < 1% (0.01), ideally < 0.1% (0.001)")
    print("Target: Index size < 10% of compressed log size (ratio < 0.10)")


def save_results(results: list[BenchmarkResult], output_path: Path):
    """Save results to JSON file."""
    data = {
        "generated_at": datetime.now().isoformat(),
        "results": [
            {
                "implementation": r.implementation_name,
                "log_file": r.log_file,
                "compressed_size_bytes": r.compressed_size_bytes,
                "uncompressed_size_bytes": r.uncompressed_size_bytes,
                "index_size_bytes": r.index_size_bytes,
                "index_size_ratio": r.index_size_ratio,
                "num_items_indexed": r.num_items_indexed,
                "num_queries": r.num_queries,
                "true_positives": r.true_positives,
                "false_positives": r.false_positives,
                "true_negatives": r.true_negatives,
                "false_negatives": r.false_negatives,
                "actual_fpp": r.actual_fpp,
                "target_fpp": r.target_fpp,
                "index_creation_time_ms": r.index_creation_time_ms,
                "query_time_ms": r.query_time_ms,
            }
            for r in results
        ]
    }

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Results saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Bloom filter benchmark')
    parser.add_argument('--logs-dir', '-l', type=Path, default=Path('logs'),
                        help='Directory containing log files')
    parser.add_argument('--output-dir', '-o', type=Path, default=Path('results'),
                        help='Output directory for results')
    parser.add_argument('--fpp', '-p', type=float, default=0.01,
                        help='Target false positive probability (default: 0.01)')
    parser.add_argument('--queries', '-q', type=int, default=10000,
                        help='Number of queries per file (default: 10000)')
    parser.add_argument('--seed', '-s', type=int, default=42,
                        help='Random seed (default: 42)')
    parser.add_argument('--list-implementations', action='store_true',
                        help='List available implementations and exit')
    args = parser.parse_args()

    if args.list_implementations:
        print("Available implementations:")
        for impl in get_all_implementations():
            status = "AVAILABLE" if impl.is_available() else "NOT AVAILABLE"
            print(f"  {impl.name}: {status}")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)

    config = BenchmarkConfig(
        logs_dir=args.logs_dir,
        output_dir=args.output_dir,
        target_fpp=args.fpp,
        num_queries=args.queries,
        random_seed=args.seed,
    )

    results = run_benchmark(config)

    if results:
        print_summary(results)
        save_results(results, args.output_dir / 'benchmark_results.json')


if __name__ == '__main__':
    main()
