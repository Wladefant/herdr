#!/usr/bin/env bash
set -euo pipefail

# Mirror the portable Ubuntu checks in .github/workflows/ci.yml.
# Workers Builds' nodejs@22 image does not include the Ubuntu CI toolchain.
export PATH="$HOME/.cargo/bin:$HOME/.bun/bin:$PATH"
if ! command -v cargo >/dev/null || ! rustc --version | grep -q '^rustc 1\.96\.1 '; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --default-toolchain 1.96.1 --component rustfmt,clippy
  . "$HOME/.cargo/env"
  export PATH="$HOME/.cargo/bin:$PATH"
fi

if ! command -v just >/dev/null; then
  cargo install --locked just
fi
if ! command -v cargo-nextest >/dev/null; then
  cargo install --locked cargo-nextest
fi
if ! command -v bun >/dev/null || [[ "$(bun --version)" != "1.3.14" ]]; then
  curl -fsSL https://bun.sh/install | bash -s "bun-v1.3.14"
  export PATH="$HOME/.bun/bin:$PATH"
fi
if ! command -v zig >/dev/null || [[ "$(zig version)" != "0.16.0" ]]; then
  zig_dir="$HOME/.cache/herdr-zig-0.16.0"
  mkdir -p "$zig_dir"
  curl -fsSL https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz |
    tar -xJ --strip-components=1 -C "$zig_dir"
  export PATH="$zig_dir:$PATH"
fi
if ! command -v python3 >/dev/null; then
  if command -v apt-get >/dev/null && [[ "$(id -u)" == "0" ]]; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y python3
  else
    echo "python3 is required for the existing maintenance and architecture tests" >&2
    exit 1
  fi
fi

cargo build --release --locked
rustup show active-toolchain
rustc --version
rustfmt --version
echo "Rust formatter config: none (no root rustfmt.toml or .rustfmt.toml)"

just lint

run_suite() {
  local suite="$1"
  shift
  local output
  output="$(mktemp)"
  if [[ "$suite" == "ubuntu-nextest-all" ]]; then
    echo "PROCESS DIAGNOSTIC before $suite (PID, PPID, PGID, SID, state, command)"
    ps -o pid,ppid,pgid,sid,stat,args -p "1,$$,${PPID}" || true
  fi
  if "$@" 2>&1 | tee "$output"; then
    python3 - "$suite" "$output" <<'PY'
import re
import sys

suite, path = sys.argv[1:]
text = open(path, encoding="utf-8").read()
nextest = re.findall(
    r"(\d+) tests? run: (\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?",
    text,
)
if nextest:
    total, passed, explicit_failed, skipped = nextest[-1]
    passed = int(passed)
    failed = int(explicit_failed or 0)
    skipped = int(skipped or 0)
    failed += int(total) - passed - failed - skipped
else:
    unittest = re.findall(r"Ran (\d+) tests? in [^\n]+\n\nOK", text)
    if unittest:
        passed, failed = int(unittest[-1]), 0
    else:
        bun = re.findall(r"(\d+) pass(?:ed)?\s*\n(\d+) fail(?:ed)?", text)
        if not bun:
            raise SystemExit(f"{suite}: could not parse a test result summary")
        passed, failed = int(bun[-1][0]), int(bun[-1][1])
if passed <= 0 or failed != 0:
    raise SystemExit(f"{suite}: {passed} passed, {failed} failed (required: pass > 0, fail = 0)")
print(f"CI-SUITE {suite}: {passed} passed, {failed} failed")
PY
  else
    local status=$?
    if [[ "$suite" == "ubuntu-nextest-all" ]]; then
      echo "PROCESS DIAGNOSTIC after $suite failure (PID, PPID, PGID, SID, state, command)"
      ps -eo pid,ppid,pgid,sid,stat,args || true
    fi
    rm -f "$output"
    return "$status"
  fi
  rm -f "$output"
}

run_suite "ubuntu-nextest-all" cargo nextest run --locked -E 'all()' \
  --status-level fail --final-status-level slow --failure-output final --success-output never
run_suite "maintenance-unittest" python3 -m unittest \
  scripts.test_agent_detection_manifest_check scripts.test_changelog \
  scripts.test_config_reference_check scripts.test_docs_translation_parity \
  scripts.test_hermes_integration_asset scripts.test_package_windows_conpty \
  scripts.test_preview scripts.test_release scripts.test_unix_installer \
  scripts.test_vendor_libghostty_vt scripts.test_vendor_portable_pty \
  scripts.test_windows_cross scripts.test_windows_input
run_suite "ui-hot-path-architecture" python3 -m unittest scripts.test_ui_hot_path_architecture
run_suite "integration-assets" bun test src/integration/assets/herdr-agent-state.test.ts
run_suite "scalar-libghostty-nextest" env LIBGHOSTTY_VT_SIMD=false LIBGHOSTTY_VT_OPTIMIZE=ReleaseSafe \
  cargo nextest run --locked -E 'package(ghostty-vt) | test(ghostty)' \
    --status-level fail --final-status-level fail --failure-output final --success-output never
