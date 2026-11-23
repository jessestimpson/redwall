#!/usr/bin/env bash
# Runs a set of model configs. See README.md for each config.
#   usage: check_all.sh [quick|long|all]
set -euo pipefail
cd "$(dirname "$0")"

QUICK_PASS="RecoverySafe RecoveryReads RecoveryStorage ResolverSafe ResolverSafeThreeEpochs ResolverSmallestGap"
# Planted bugs: each config must produce a counterexample.
QUICK_FAIL="RecoveryAckReplicasOnly RecoveryFewReplies RecoveryNoCas RecoveryNoReport RecoveryFewConfirms RecoveryNoRollback RecoveryNoInFlightLimit ResolverNoCheck ResolverNoGap"
# Reachability: each config must reach the state that the invariant of the
# config says is never reached.
QUICK_REACH="RecoveryReachThirdEpoch RecoveryReachDiscard RecoveryReachCopy RecoveryReachRollback RecoveryReachReadSecondEpoch RecoveryReachStoreSecondEpoch ResolverReachConflict ResolverReachTooOldInEpoch ResolverReachTooOldAcrossEpochs ResolverReachCommitSecondEpoch"
LONG_PASS="RecoverySafeThreeEpochs RecoveryStorageThreeEpochs"

run() { # expectation configs...
  local expect="$1"; shift
  for cfg in "$@"; do
    echo "=== $cfg ($expect)"
    ./check.sh "$cfg" "$expect" | grep -E "No error has been found|Invariant .* is violated|distinct states found, |Finished in|^OK:|^ERROR"
  done
}

case "${1:-quick}" in
  quick) run pass $QUICK_PASS; run fail $QUICK_FAIL $QUICK_REACH ;;
  long) run pass $LONG_PASS ;;
  all) run pass $QUICK_PASS $LONG_PASS; run fail $QUICK_FAIL $QUICK_REACH ;;
  *) echo "usage: check_all.sh [quick|long|all]" >&2; exit 2 ;;
esac
