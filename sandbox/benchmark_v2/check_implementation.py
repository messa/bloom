#!/usr/bin/env python3
"""
Check implementation correctness for bloom filter implementations.

Usage:
    cd sandbox/benchmark_v2 && ./check_implementation.py implementations/trivial_python_bloom/trivial_python_bloom.py
"""

import lzma
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def create_test_file(directory: Path, filename: str, content: bytes) -> Path:
    """Create a compressed test file."""
    filepath = directory / filename
    with lzma.open(filepath, 'wb') as f:
        f.write(content)
    return filepath


def run_implementation(impl_path: str, index_dir: str, args: list[str]) -> int:
    """Run the implementation with given arguments and return the exit code."""
    env = os.environ.copy()
    env['INDEX_DIR'] = index_dir

    result = subprocess.run(
        [sys.executable, impl_path] + args,
        env=env,
        capture_output=True,
        text=True
    )

    if result.stderr:
        print(f"  stderr: {result.stderr.strip()}")

    return result.returncode


def test_basic_functionality(impl_path: str, tmpdir: Path) -> bool:
    """Test basic add and check functionality."""
    print("Testing basic functionality...")

    index_dir = tmpdir / 'index'
    data_dir = tmpdir / 'data'
    data_dir.mkdir()

    # Create test file with known content
    test_content = b"Hello world, this is a test file with some searchable content."
    test_file = create_test_file(data_dir, 'test.log.xz', test_content)

    # Test add command
    print("  Running 'add' command...")
    ret = run_implementation(impl_path, str(index_dir), ['add', str(test_file)])
    if ret != 0:
        print(f"  FAIL: 'add' command returned {ret}, expected 0")
        return False
    print("  OK: 'add' command succeeded")

    # Check that index file was created (could be .bitmap, .bloom, etc.)
    index_files = list(index_dir.glob('test.log.xz.*'))
    if not index_files:
        print(f"  FAIL: No index file created in {index_dir}")
        return False
    print(f"  OK: Index file created: {index_files[0].name}")

    # Test check command with term that exists
    print("  Running 'check' command with existing term 'Hello'...")
    ret = run_implementation(impl_path, str(index_dir), ['check', str(test_file), 'Hello'])
    if ret != 0:
        print(f"  FAIL: 'check' for 'Hello' returned {ret}, expected 0")
        return False
    print("  OK: 'check' for existing term returned 0")

    # Test check command with term that exists (longer)
    print("  Running 'check' command with existing term 'searchable'...")
    ret = run_implementation(impl_path, str(index_dir), ['check', str(test_file), 'searchable'])
    if ret != 0:
        print(f"  FAIL: 'check' for 'searchable' returned {ret}, expected 0")
        return False
    print("  OK: 'check' for existing term returned 0")

    # Test check command with term that definitely doesn't exist
    # Using a very unique string that shares no n-grams with the content
    print("  Running 'check' command with non-existing term 'xyz123qwerty789'...")
    ret = run_implementation(impl_path, str(index_dir), ['check', str(test_file), 'xyz123qwerty789'])
    if ret != 1:
        print(f"  FAIL: 'check' for non-existing term returned {ret}, expected 1")
        return False
    print("  OK: 'check' for non-existing term returned 1")

    return True


def test_error_handling(impl_path: str, tmpdir: Path) -> bool:
    """Test error handling."""
    print("Testing error handling...")

    index_dir = tmpdir / 'index_error'

    # Test check command with non-existent index
    print("  Running 'check' command with non-existent index...")
    ret = run_implementation(impl_path, str(index_dir), ['check', 'nonexistent.log.xz', 'test'])
    if ret != 2:
        print(f"  FAIL: 'check' for non-existent index returned {ret}, expected 2")
        return False
    print("  OK: Error handling returned exit code 2")

    return True


def main():
    if len(sys.argv) != 2:
        print("Usage: check_implementation.py <implementation_script>", file=sys.stderr)
        sys.exit(1)

    impl_path = sys.argv[1]

    if not os.path.exists(impl_path):
        print(f"Error: Implementation not found: {impl_path}", file=sys.stderr)
        sys.exit(1)

    # Make path absolute
    impl_path = os.path.abspath(impl_path)

    print(f"Testing implementation: {impl_path}")
    print()

    all_passed = True

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        if not test_basic_functionality(impl_path, tmpdir):
            all_passed = False
        print()

        if not test_error_handling(impl_path, tmpdir):
            all_passed = False
        print()

    if all_passed:
        print("All tests PASSED!")
        sys.exit(0)
    else:
        print("Some tests FAILED!")
        sys.exit(1)


if __name__ == '__main__':
    main()
