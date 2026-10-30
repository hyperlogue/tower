#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
pinned=$(awk -F'"' '/^channel[[:space:]]*=/ { print $2 }' rust-toolchain.toml)
actual=$(rustc --version | cut -d' ' -f2)

if [[ "$pinned" != "$actual" ]]; then
  printf 'Toolchain mismatch: rust-toolchain.toml=%s, active rustc=%s\n' "$pinned" "$actual" >&2
  exit 1
fi

printf 'Rust toolchain: %s\n' "$actual"
