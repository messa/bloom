#!/bin/bash
set -e

cd "$(dirname "$0")"

failed=0

for impl in implementations/*/*_ngram implementations/*/*_ngram.py; do
    [ -e "$impl" ] || continue  # Skip if no match
    echo "========================================"
    echo "Checking: $impl"
    echo "========================================"
    if ./check_implementation.py "$impl"; then
        echo ""
    else
        failed=1
        echo ""
    fi
done

echo "========================================"
if [ $failed -eq 0 ]; then
    echo "All implementations PASSED!"
else
    echo "Some implementations FAILED!"
    exit 1
fi
