#!/bin/bash
set -e

cd "$(dirname "$0")"

./check_implementation.py implementations/trivial_python_bloom/trivial_python_bloom_ngram.py
./check_implementation.py implementations/trivial_cpp_bloom/trivial_cpp_bloom_ngram
./check_implementation.py implementations/python_pybloomfiltermmap3/sample_pybloomfiltermmap3_ngram.py
./check_implementation.py implementations/python_rbloom/sample_rbloom_ngram.py
./check_implementation.py implementations/python_blooms/sample_blooms_ngram.py
./check_implementation.py implementations/python_fastbloom/sample_fastbloom_ngram.py
./check_implementation.py implementations/rust_fastbloom/rust_fastbloom_ngram

echo "========================================"
echo "All implementations PASSED!"
