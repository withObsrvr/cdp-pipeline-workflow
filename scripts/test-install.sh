#!/bin/bash
# Test installation script for FlowCTL
# Usage: ./test-install.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
test_pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((TESTS_PASSED++))
}

test_fail() {
    echo -e "${RED}✗${NC} $1"
    ((TESTS_FAILED++))
}

test_info() {
    echo -e "${YELLOW}→${NC} $1"
}

# Test 1: Check if flowctl is in PATH
test_info "Testing if flowctl is accessible..."
if command -v flowctl >/dev/null 2>&1; then
    test_pass "flowctl found in PATH"
else
    test_fail "flowctl not found in PATH"
    echo "Please ensure flowctl is installed and in your PATH"
    exit 1
fi

# Test 2: Check version command
test_info "Testing version command..."
if flowctl version >/dev/null 2>&1; then
    VERSION_OUTPUT=$(flowctl version 2>&1)
    test_pass "Version command works"
    echo "  Version output: $VERSION_OUTPUT"
else
    test_fail "Version command failed"
fi

# Test 3: Check help command
test_info "Testing help command..."
if flowctl --help >/dev/null 2>&1; then
    test_pass "Help command works"
else
    test_fail "Help command failed"
fi

# Test 4: Check run help
test_info "Testing run help command..."
if flowctl run --help >/dev/null 2>&1; then
    test_pass "Run help command works"
else
    test_fail "Run help command failed"
fi

# Test 5: Test config validation (dry run)
test_info "Testing config validation..."
# Create a simple test config
TEST_CONFIG=$(mktemp -t flowctl-test-XXXXXX.yaml)
cat > "$TEST_CONFIG" << 'EOF'
version: "1.0"
pipelines:
  - name: test-pipeline
    source:
      type: captive-core
      config:
        network: testnet
    processors:
      - type: latest-ledger
    consumers:
      - type: stdout
EOF

if flowctl run --dry-run "$TEST_CONFIG" >/dev/null 2>&1; then
    test_pass "Config validation works"
else
    test_fail "Config validation failed"
fi

rm -f "$TEST_CONFIG"

# Test 6: Check binary permissions
test_info "Testing binary permissions..."
FLOWCTL_PATH=$(which flowctl)
if [ -x "$FLOWCTL_PATH" ]; then
    test_pass "Binary has execute permissions"
else
    test_fail "Binary lacks execute permissions"
fi

# Test 7: Check installation directory structure
test_info "Testing installation directory..."
if [ -n "$FLOWCTL_INSTALL_DIR" ]; then
    INSTALL_DIR="$FLOWCTL_INSTALL_DIR"
else
    INSTALL_DIR="$HOME/.flowctl"
fi

if [ -d "$INSTALL_DIR/bin" ]; then
    test_pass "Installation directory structure is correct"
else
    test_fail "Installation directory structure is incorrect"
fi

# Summary
echo
echo "========================================="
echo "Test Summary:"
echo "  Passed: $TESTS_PASSED"
echo "  Failed: $TESTS_FAILED"
echo "========================================="

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! FlowCTL is properly installed.${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Please check the installation.${NC}"
    exit 1
fi