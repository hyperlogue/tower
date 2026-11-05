#!/usr/bin/env bash
set -euo pipefail
tower_test_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export TOWER_TEST_BIN="${TOWER_TEST_BIN:-$tower_test_root/target/debug/tower-cli}"
export TRACETAPE_ROOT="${TRACETAPE_ROOT:-$tower_test_root/../tracetape}"
TOWER_TEST_CODEX=$(readlink -f "$(command -v codex)")
TOWER_TEST_CLAUDE=$(readlink -f "$(command -v claude)")
export TOWER_TEST_CODEX TOWER_TEST_CLAUDE
test -x "$TOWER_TEST_BIN"
test -x "$(dirname "$TOWER_TEST_BIN")/tower"
test -x "$(dirname "$TOWER_TEST_BIN")/tower-terminal-pool"
test -f "$TRACETAPE_ROOT/src/index.ts"
export TOWER_TEST_SCRIPT="$tower_test_root/tests/e2e/replay.mjs"
# shellcheck disable=SC2016
exec unshare --user --map-root-user --net bash -c '
  set -euo pipefail
  ip link set lo up
  test -z "$(ip route show)"
  export TOWER_TEST_OFFLINE=1
  exec bun "$TOWER_TEST_SCRIPT"
'
