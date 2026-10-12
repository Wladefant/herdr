#!/usr/bin/env bash
set -euo pipefail

# Mirror the portable Ubuntu checks in .github/workflows/ci.yml.
cargo build --release --locked
just lint

run_suite() {
  local suite="$1"
  shift
  local output
  output="$(mktemp)"
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
