#!/usr/bin/env bash
set -euo pipefail

# Mirror the portable Ubuntu checks in .github/workflows/ci.yml.
cargo build --release --locked
just lint
just ci-tests 'all()'
LIBGHOSTTY_VT_SIMD=false LIBGHOSTTY_VT_OPTIMIZE=ReleaseSafe \
  cargo nextest run --locked -E 'package(ghostty-vt) | test(ghostty)' \
    --status-level fail --final-status-level fail --failure-output final --success-output never
