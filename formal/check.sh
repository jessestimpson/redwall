#!/usr/bin/env bash
# Runs TLC on one model config.
#   usage: check.sh <ConfigBasenameWithout.cfg> [pass|fail]
# With `fail`, the script requires TLC to find an invariant violation. Use
# `fail` for the configs with a planted bug and for the reachability configs.
# The TLC jar is $TLA2TOOLS_JAR if it is set, and the jar in formal/vendor
# otherwise.
set -euo pipefail
cd "$(dirname "$0")"

CFG="${1:?usage: check.sh <config-basename> [pass|fail]}"
EXPECT="${2:-pass}"
JAR="${TLA2TOOLS_JAR:-vendor/tla2tools-v1.8.0.jar}"
OUT="$(mktemp)"
META="$(mktemp -d)"
trap 'rm -rf "$OUT" "$META"' EXIT

set +e
# -deadlock disables deadlock checking. A bounded model has no enabled action
# after every epoch and every LSN is used. That state is not an error.
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
  -workers auto -deadlock -noGenerateSpecTE -metadir "$META" \
  -config "${CFG}.cfg" MCRecovery.tla 2>&1 | tee "$OUT"
STATUS=${PIPESTATUS[0]}
set -e

if [ "$EXPECT" = "pass" ]; then
  exit "$STATUS"
fi
if [ "$STATUS" -ne 0 ] && grep -Eq "Invariant .* is violated" "$OUT"; then
  echo "OK: expected invariant violation was found"
  exit 0
fi
echo "ERROR: expected an invariant violation, but TLC exited $STATUS without one"
exit 1
