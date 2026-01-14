#!/usr/bin/env bash
# Lint and format script for LeanSQLite C bindings

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
CHECK_ONLY=false
LINT_ONLY=false
FORMAT_ONLY=false

for arg in "$@"; do
  case $arg in
    --check)
      CHECK_ONLY=true
      ;;
    --lint-only)
      LINT_ONLY=true
      ;;
    --format-only)
      FORMAT_ONLY=true
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Lint and format C bindings for LeanSQLite"
      echo ""
      echo "OPTIONS:"
      echo "  --check         Check formatting without modifying files"
      echo "  --lint-only     Run only clang-tidy (no formatting)"
      echo "  --format-only   Run only clang-format (no linting)"
      echo "  --help, -h      Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                    # Lint and format"
      echo "  $0 --check            # Lint and check formatting (CI mode)"
      echo "  $0 --lint-only        # Only run linter"
      echo "  $0 --format-only      # Only format code"
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $arg${NC}"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Find clang-tidy (check PATH first, then Homebrew LLVM location)
find_clang_tool() {
  local tool_name=$1
  local tool_path=""

  if command -v "$tool_name" &> /dev/null; then
    tool_path="$tool_name"
  elif command -v brew &> /dev/null; then
    local brew_llvm_bin
    brew_llvm_bin="$(brew --prefix llvm 2>/dev/null)/bin/$tool_name"
    if [ -f "$brew_llvm_bin" ]; then
      tool_path="$brew_llvm_bin"
    fi
  fi

  echo "$tool_path"
}

CLANG_TIDY=$(find_clang_tool "clang-tidy")
CLANG_FORMAT=$(find_clang_tool "clang-format")

# Check for missing tools
MISSING_TOOLS=()
if [ "$FORMAT_ONLY" != true ] && [ -z "$CLANG_TIDY" ]; then
  MISSING_TOOLS+=("clang-tidy")
fi
if [ "$LINT_ONLY" != true ] && [ -z "$CLANG_FORMAT" ]; then
  MISSING_TOOLS+=("clang-format")
fi

if [ ${#MISSING_TOOLS[@]} -gt 0 ]; then
  echo -e "${RED}Error: Missing tools: ${MISSING_TOOLS[*]}${NC}"
  echo "Install with:"
  echo "  - macOS: brew install llvm"
  echo "  - Ubuntu/Debian: sudo apt install clang-tidy clang-format"
  echo "  - Fedora: sudo dnf install clang-tools-extra"
  exit 1
fi

# Get Lean include directory
LEAN_INCLUDE=$(lean --print-prefix)/include

# Get SQLite include directory (local)
SQLITE_INCLUDE="$(pwd)/bindings"

EXIT_CODE=0

# Run linter
if [ "$FORMAT_ONLY" != true ]; then
  echo -e "${YELLOW}Running clang-tidy on C bindings...${NC}"
  echo "Using: $CLANG_TIDY"

  if "$CLANG_TIDY" bindings/leansqlite.c \
    -header-filter='bindings/leansqlite\.(c|h)' \
    -- \
    -I"$LEAN_INCLUDE" \
    -I"$SQLITE_INCLUDE" \
    -std=c11 \
    -D__STDC_WANT_LIB_EXT1__=1; then
    echo -e "${GREEN}✓ Linting passed${NC}"
  else
    echo -e "${RED}✗ Linting failed${NC}"
    EXIT_CODE=1
  fi
  echo ""
fi

# Run formatter
if [ "$LINT_ONLY" != true ]; then
  if [ "$CHECK_ONLY" = true ]; then
    echo -e "${YELLOW}Checking C bindings formatting (dry-run)...${NC}"
    echo "Using: $CLANG_FORMAT"

    if "$CLANG_FORMAT" --dry-run --Werror bindings/leansqlite.c 2>&1; then
      echo -e "${GREEN}✓ Code is properly formatted${NC}"
    else
      echo -e "${RED}✗ Code needs formatting${NC}"
      echo -e "${BLUE}Run './lint-format-c.sh --format-only' to format the code${NC}"
      EXIT_CODE=1
    fi
  else
    echo -e "${YELLOW}Formatting C bindings...${NC}"
    echo "Using: $CLANG_FORMAT"

    "$CLANG_FORMAT" -i bindings/leansqlite.c
    echo -e "${GREEN}✓ Formatted bindings/leansqlite.c${NC}"
  fi
fi

exit $EXIT_CODE
