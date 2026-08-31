#!/usr/bin/env bash
# Measure src/ kernel line coverage (not src/testing, not tests/).
# Fail if total % drops below coverage/BASELINE, or if a new/changed
# src/ kernel line is uncovered (unless allowlisted).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASELINE="${COVERAGE_BASELINE:-$ROOT/coverage/BASELINE}"
LCOV="${COVERAGE_LCOV:-$ROOT/coverage/lcov.info}"
mkdir -p "$ROOT/coverage"

if [[ ! -f "$BASELINE" ]]; then
  echo "missing $BASELINE" >&2
  exit 2
fi

FLOOR="$(awk -F= '/^floor_lines_pct=/{print $2}' "$BASELINE")"
FAIL_UNDER="$(awk -F= '/^fail_under_lines=/{print $2}' "$BASELINE")"
if [[ -z "$FLOOR" || -z "$FAIL_UNDER" ]]; then
  echo "coverage/BASELINE must set floor_lines_pct and fail_under_lines" >&2
  exit 2
fi

IGNORE='src/testing/|\.cargo/|/tests/|/examples/'

TESTS=(
  --lib
  --test adversarial
  --test catalog
  --test consumer
  --test graph
  --test resilience
  --test scenarios
  --test stress
  --test stress_uneven
  --test structure
  --test workloads
)

if [[ "${COVERAGE_SKIP_RUN:-}" != "1" ]]; then
  # Native llvm-cov `--fail-under-lines` ANDs per-CGU mappings (lib tests vs
  # integration tests) and treats closing-brace regions as missed even when
  # lcov DA hits are 100% after OR-merge. The python gate is the 100% check.
  cargo llvm-cov "${TESTS[@]}" \
    --lcov --output-path "$LCOV" \
    --ignore-filename-regex "$IGNORE" \
    -- --test-threads=1
fi

python3 "$ROOT/scripts/coverage_gate.py" \
  --baseline "$BASELINE" \
  --lcov "$LCOV" \
  --floor "$FLOOR" \
  --base "${COVERAGE_DIFF_BASE:-}"
