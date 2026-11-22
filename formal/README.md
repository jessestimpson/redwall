# Formal model

[Recovery.tla](Recovery.tla) models the logging protocol and the recovery of
the transaction system of FoundationDB (paper, sections 2.4.3, 2.4.4 and
2.5). [MCRecovery.tla](MCRecovery.tla) adds the constants and the
invariants for model checking.

## Contents of the model

| Part | In the model |
|---|---|
| Coordinated state | One register with two atomic operations: `LockCState` and `WriteCState` |
| Sequencer | One for each epoch. It assigns LSNs and performs the recovery |
| Proxy | One for each epoch. It keeps a Known Committed Version and answers clients |
| LogServer | One for each element of `Logs`, in each epoch. It keeps a Durable Version, a Known Committed Version and the bodies of records |
| Log record | An LSN, the previous LSN, the Known Committed Version of the Proxy, and the set of `K` LogServers that store the body |

The model does not contain Resolvers, StorageServers, tags, the recovery
transaction, the election of a ClusterController or the removal of old
LogServers.

## Recovery in the model

1. `LockCState(n)`: the Sequencer of epoch `n` raises the recovery count
   from `n - 1` to `n`, and reads the current log system.
2. `LockLog(n, l)`: the Sequencer stops LogServer `l` of the old epoch. The
   LogServer answers with its Durable Version and its Known Committed
   Version.
3. `Recruit(n)`: with `MinReplies` answers, the Sequencer computes the
   previous epoch's end version (PEV) as the maximum of the Known Committed
   Versions, and the Recovery Version (RV) as the minimum of the Durable
   Versions.
4. `WriteCState(n)`: the Sequencer writes the new log system, if the
   recovery count is still `n`. The records in `PEV + 1 .. RV` are copied to
   the new LogServers. The first LSN of epoch `n` is `RV + 1`.

## Invariants

| Invariant | Meaning |
|---|---|
| `Durable` | When an epoch has ended, every LSN that the Proxy of the epoch acknowledged is at or below the Recovery Version |
| `NoLoss` | Every record that a recovery must copy has its body on an old LogServer that answered |
| `UniqueVersions` | An LSN is acknowledged in one epoch only |
| `KcvSound` | The Known Committed Version of a LogServer is an acknowledged LSN, or the PEV of its epoch |

## Configs and results

All configs have 3 LogServers and a replication degree of 2. The results are
from one machine with 10 processors, on 2026-11-21.

| Config | Epochs | Versions | Expected result | Result | Distinct states | Time |
|---|---|---|---|---|---|---|
| `RecoverySafe` | 2 | 3 | No violation | No violation | 4,686,198 | 26 s |
| `RecoverySafeThreeEpochs` | 3 | 2 | No violation | No violation | 35,667,136 | 4 min 37 s |
| `RecoveryAckReplicasOnly` | 2 | 2 | `Durable` is violated | Violated | | under 1 s |
| `RecoveryFewReplies` | 2 | 2 | `NoLoss` is violated | Violated | | under 1 s |
| `RecoveryNoCas` | 3 | 1 | `UniqueVersions` is violated | Violated | | 2 s |
| `RecoveryReachThirdEpoch` | 3 | 1 | The third epoch acknowledges a transaction | Reached | | under 1 s |
| `RecoveryReachDiscard` | 2 | 2 | A recovery discards a persisted record | Reached | | under 1 s |
| `RecoveryReachCopy` | 2 | 2 | A recovery copies a record to the new epoch | Reached | | under 1 s |

### The planted bugs

- **`RecoveryAckReplicasOnly`** (`AckAll = FALSE`). A Proxy answers a client
  after the `K` LogServers that store the body have persisted the LSN. A
  LogServer that received the empty body has a lower Durable Version. A
  recovery that hears from that LogServer chooses a Recovery Version below
  the acknowledged LSN. A Proxy must therefore wait for every LogServer of
  the epoch. The recovery document defines the Known Committed Version in
  the same way: the version that is durable on all LogServers.
- **`RecoveryFewReplies`** (`MinReplies = 1`). The paper requires more than
  `m - k` answers from `m` LogServers. With fewer answers, the LogServers
  that answered can all lack the body of a record.
- **`RecoveryNoCas`** (`CasWrite = FALSE`). Two Sequencers recover the same
  epoch, and both write the coordinated state. Both new epochs acknowledge
  the same LSN.

## Commands

```
./check.sh RecoverySafe            # one config that must pass
./check.sh RecoveryNoCas fail      # one config that must find a violation
./check_all.sh quick               # every config except the long one
./check_all.sh long                # RecoverySafeThreeEpochs
```

## Limitations

- The bounds are small: 3 LogServers, at most 3 epochs and 3 versions.
- The model copies each record in `PEV + 1 .. RV` to every new LogServer.
  FoundationDB copies a record to the LogServers of its tags.
- The model has one Proxy for each epoch.
- A Sequencer whose lock fails is not modelled, because that Sequencer
  changes nothing.
- The model checks safety only. It has no liveness property.
