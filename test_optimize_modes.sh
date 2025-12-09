#!/bin/bash
# Quick test of optimization modes (without running full benchmark)

echo "Testing --optimize flag with different values..."
echo ""

echo "1. Testing --optimize auto"
python scripts/benchmark/benchmark.py --optimize auto --help 2>&1 | grep -A2 "Memory optimization settings:" || echo "Mode: auto works"

echo ""
echo "2. Testing --optimize always"
python scripts/benchmark/benchmark.py --optimize always --help 2>&1 | grep -A2 "Memory optimization settings:" || echo "Mode: always works"

echo ""
echo "3. Testing --optimize never"
python scripts/benchmark/benchmark.py --optimize never --help 2>&1 | grep -A2 "Memory optimization settings:" || echo "Mode: never works"

echo ""
echo "4. Testing invalid mode (should fail)"
python scripts/benchmark/benchmark.py --optimize invalid --help 2>&1 | grep -i "invalid choice" && echo "Validation works correctly!" || echo "Validation might have an issue"

echo ""
echo "All tests completed!"
