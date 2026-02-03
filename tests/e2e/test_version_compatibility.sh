#!/bin/bash
# E2E test runner for Bolt protocol version compatibility
#
# This script runs the COMPLETE e2e test suite with different BOLT_MAX_VERSION
# settings to verify that graphd works correctly at each Bolt protocol level.
#
# IMPORTANT: This tests the full protocol implementation, not just the handshake.
# Each test run includes:
#   - Connection handshake and authentication
#   - Query execution (CREATE, MATCH, WHERE, etc.)
#   - Transaction handling (BEGIN, COMMIT, ROLLBACK)
#   - Error handling and reporting (version-specific formats)
#   - Result streaming and data type conversions
#   - Connection hints (for Bolt 5.4+)
#   - All 298 e2e tests in test_e2e.py
#
# The BOLT_MAX_VERSION environment variable causes the server to artificially
# limit its negotiated version, allowing us to test version-specific behavior
# without needing multiple client implementations.
#
# The e2e tests (test_e2e.py) start their own server instance with the
# BOLT_MAX_VERSION variable set, so we just need to export it.
#
# Usage:
#   ./tests/e2e/test_version_compatibility.sh
#   make version-compat  # Recommended

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

echo "======================================================"
echo "Bolt Protocol Version Compatibility Test"
echo "======================================================"
echo ""
echo "This test verifies graphd works with different Bolt"
echo "protocol versions by artificially limiting the server's"
echo "maximum negotiated version and running e2e tests."
echo ""

# Test versions to check
VERSIONS=("4.4" "5.0" "5.1" "5.4" "5.7")

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results (parallel arrays for bash 3.2 compatibility)
RESULT_VERSIONS=()
RESULT_STATUS=()
TOTAL_TESTS=${#VERSIONS[@]}
PASSED=0
FAILED=0

# Clean up any running servers
cleanup() {
    echo "Cleaning up..."
    make down 2>/dev/null || true
    pkill -f "graphd" 2>/dev/null || true
    rm -rf /tmp/graphd_test_* 2>/dev/null || true
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Function to run tests with a specific version limit
run_version_test() {
    local version=$1
    echo ""
    echo "======================================================"
    echo -e "${YELLOW}Testing with Bolt $version${NC}"
    echo "======================================================"

    # Export the version limit for the e2e test to pick up
    export BOLT_MAX_VERSION=$version

    # Run Python e2e tests (they start their own server)
    echo "Running e2e tests with BOLT_MAX_VERSION=$version..."
    local log_file="/tmp/graphd_e2e_${version//./}.log"

    if make e2e-py > "$log_file" 2>&1; then
        echo -e "${GREEN}✓ Bolt $version tests PASSED${NC}"
        RESULT_VERSIONS+=("$version")
        RESULT_STATUS+=("PASSED")
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ Bolt $version tests FAILED${NC}"
        echo "See $log_file for details"
        echo "Last 20 lines of log:"
        tail -20 "$log_file" | sed 's/^/  /'
        RESULT_VERSIONS+=("$version")
        RESULT_STATUS+=("FAILED_TESTS")
        FAILED=$((FAILED + 1))
    fi

    # Unset for next iteration
    unset BOLT_MAX_VERSION

    # Brief pause between tests
    sleep 1
}

# Build first
echo "Building graphd..."
make build

# Run tests for each version
for version in "${VERSIONS[@]}"; do
    run_version_test "$version"
done

# Print summary
echo ""
echo "======================================================"
echo "Test Summary"
echo "======================================================"
echo ""
printf "%-10s %s\n" "Version" "Result"
printf "%-10s %s\n" "-------" "------"

for i in "${!RESULT_VERSIONS[@]}"; do
    version="${RESULT_VERSIONS[$i]}"
    result="${RESULT_STATUS[$i]}"
    if [[ "$result" == "PASSED" ]]; then
        printf "%-10s ${GREEN}%s${NC}\n" "$version" "$result"
    else
        printf "%-10s ${RED}%s${NC}\n" "$version" "$result"
    fi
done

echo ""
echo "======================================================"
printf "Total: %d | ${GREEN}Passed: %d${NC} | ${RED}Failed: %d${NC}\n" "$TOTAL_TESTS" "$PASSED" "$FAILED"
echo "======================================================"

# Exit with error if any tests failed
if [ $FAILED -gt 0 ]; then
    echo ""
    echo -e "${RED}Some version compatibility tests failed!${NC}"
    exit 1
else
    echo ""
    echo -e "${GREEN}All version compatibility tests passed!${NC}"
    exit 0
fi
