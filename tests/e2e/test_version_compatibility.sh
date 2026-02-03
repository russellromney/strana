#!/bin/bash
# E2E test runner for Bolt protocol version compatibility
#
# This script runs e2e tests with different BOLT_MAX_VERSION settings
# to verify that graphd works correctly with clients at each protocol level.
#
# Usage:
#   ./tests/e2e/test_version_compatibility.sh

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

# Track results
declare -a RESULTS
TOTAL_TESTS=${#VERSIONS[@]}
PASSED=0
FAILED=0

# Function to run tests with a specific version limit
run_version_test() {
    local version=$1
    echo ""
    echo "======================================================"
    echo -e "${YELLOW}Testing with Bolt $version${NC}"
    echo "======================================================"

    # Start server with version limit
    echo "Starting graphd with BOLT_MAX_VERSION=$version..."
    BOLT_MAX_VERSION=$version make up &
    SERVER_PID=$!

    # Wait for server to be ready
    sleep 3

    # Check if server is running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo -e "${RED}✗ Server failed to start${NC}"
        RESULTS[$version]="FAILED_START"
        FAILED=$((FAILED + 1))
        return 1
    fi

    # Run Python e2e tests
    echo "Running e2e tests..."
    if make e2e-py > /tmp/graphd_e2e_${version}.log 2>&1; then
        echo -e "${GREEN}✓ Bolt $version tests PASSED${NC}"
        RESULTS[$version]="PASSED"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ Bolt $version tests FAILED${NC}"
        echo "See /tmp/graphd_e2e_${version}.log for details"
        RESULTS[$version]="FAILED_TESTS"
        FAILED=$((FAILED + 1))
    fi

    # Stop server
    echo "Stopping graphd..."
    make down || true
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true

    # Give it a moment to fully shut down
    sleep 1
}

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

for version in "${VERSIONS[@]}"; do
    result="${RESULTS[$version]}"
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
