# The cluster

## The ClusterController

A node that can run roles has one worker
([redwall_worker](../src/redwall_worker.erl)). A node can also run one
candidate for the ClusterController
([redwall_cluster_controller](../src/redwall_cluster_controller.erl)).

Election:

1. The coordinated state has the term and the process of the
   ClusterController.
2. A candidate reads the coordinated state every `poll_ms` (default
   1,000 ms) and monitors the ClusterController of the term.
3. If the term has no ClusterController, or the monitor of the
   ClusterController has ended, the candidate proposes itself with the
   term that it read.
4. The coordinated state accepts one proposal for a term. The candidate
   of that proposal is the leader of the next term.
5. The leader reads the coordinated state every `poll_ms`. If the
   coordinated state has another term, the leader is a candidate again.

Two processes can be the leader at the same time, for at most `poll_ms`
plus the time of a read. Each of them can recruit a Sequencer. The
recovery count of the coordinated state then lets one Sequencer write, as
for any two recoveries at the same time. FoundationDB has the same
property (`design/recovery-internals.md`).

Recruitment:

1. Each worker reads the ClusterController from the coordinated state
   and registers with it. A worker has the class `log`, `stateless` or
   `unset`.
2. The leader recruits when it has no epoch and at least `min_workers`
   workers (default 1). A cluster of several nodes sets `min_workers` to
   2 or more. With the default, the leader recruits when the first
   worker has registered, and every LogServer of the first epoch is on
   that worker.
3. With several workers, each LogServer of an epoch is on a worker of
   its own: an epoch has `logs` LogServers, or one for each worker if
   the leader has fewer workers, and `replication` is at most the number
   of LogServers. Before 2026-11-23 the leader used a worker for two
   LogServers when it had fewer workers than `logs`, and the stop of one
   node could then end every LogServer with a record. With one worker,
   every LogServer is on that worker. The leader prefers the class `log`
   for a LogServer. It chooses a worker for the process of the
   epoch, and prefers the class `stateless`. Each attempt starts at a
   different worker.
4. The leader starts the process of the epoch on its worker. The process
   performs the recovery, and starts the LogServers on their workers.
5. The leader sends the epoch and the process of the epoch to every
   worker. `redwall_worker:db_info/0` returns them on each node with a
   worker.

The leader monitors the process of the epoch and the LogServers. When
one of them stops, or the connection to its node ends, or the
recovery fails, the leader recruits again after `retry_ms` (default
500 ms). The leader also recruits again when the recovery count of the
coordinated state is above its epoch, because another Sequencer has then
started a recovery.

A worker sends its registration every `poll_ms`, and the leader answers
with the epoch, the Sequencer and the Proxy. A worker that missed a
message receives them within `poll_ms`. A worker, a candidate and the
leader read the coordinated state in a separate process, so each of them
answers its requests while a read waits.

## Start

A node runs redwall from the environment of the application. The
application starts a worker and a StorageServer if the environment has
their options.

| Key of the environment | Value | Process |
|---|---|---|
| `worker` | The options of `redwall_worker:start_link/1`: `coordinators`, `class`, `candidate` for a candidate for the ClusterController on the node, and `name` for a node that runs the workers of several clusters | The worker of the node. With `candidate`, the worker starts the candidate, and the two processes stop together |
| `storage` | The options of `redwall_storage:start_link/1`: `coordinators` | The StorageServer of the node |
| `storage_engine` | `ets` (the default), or a module with the behaviour `redwall_storage_engine`. Every node of a cluster has the same value. See [The storage engine](storage-server.md#the-storage-engine) | None. The supervisor gives the value to the StorageServer of the node |
| `coordinator` | The options of `redwall_coordinator:start_link/1`: `id` and `dir`, both required | The coordinator of the node: one server of the peace cluster with the coordinated state. The supervisor starts it before the worker and the StorageServer |

Example, in `sys.config`, for a node that runs every role:

```erlang
[
    {redwall, [
        {worker, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}], candidate => #{}}},
        {storage, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}]}}
    ]}
].
```

`coordinators` are the servers of a peace cluster that runs
`redwall_cstate:machine/0`.

### Coordinators

A coordinator keeps the coordinated state on a disk, as a coordinator of
FoundationDB does. [redwall_coordinator](../src/redwall_coordinator.erl)
starts a server of peace with the write-ahead log of peace
(`peace_storage_wal`), and the option `dir` is required:

```erlang
{redwall, [
    {coordinator, #{id => {c1, 'a@host'}, dir => "/var/lib/redwall/coordinator"}},
    {worker, #{coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}], candidate => #{}}}
]}
```

1. Start the application on each node with a coordinator.
2. Call `redwall_coordinator:form([{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}])`
   once. peace stores the configuration of the cluster on the disk of
   each coordinator.

- A coordinator that starts again reads its data from `dir`, and is the
  same member of the cluster as before. The second step is not repeated.
- Without `dir`, the coordinator does not start, and the error is
  `{missing_option, dir}`. The options `storage` and `machine` are not
  allowed.
- A peace cluster that a caller starts with `peace:start_cluster/3` can
  still be named in `coordinators`. The tests of redwall do this with
  the storage in memory.

### Start time

The start of one node with all roles takes 35 ms to 46 ms, measured on
2026-11-23 with `peace:trigger_election/1`. Before the changes below it
took about 3 s.

- A worker without an answer of a ClusterController, a candidate without
  an answer of the coordinated state, and a StorageServer without a log
  system read the coordinated state after 10 ms, and double the time
  with each read up to their poll interval. Before, each of them waited
  for its poll interval, which is 1,000 ms for the worker and 500 ms for
  the StorageServer.
- A peace cluster has no leader until an election timeout has passed.
  `peace:trigger_election/1` starts an election at once. A caller that
  starts a peace cluster for redwall calls it after
  `peace:start_cluster/3`.
- The same early reads shorten the time after a recovery in which a
  read of a client waits: a StorageServer follows the epoch after a
  recovery within some 10 ms. Before, it followed at its poll interval.

On such a node, `redwall:open/0` returns a database:

```erlang
Db = redwall:open(),
ok = redwall:transactional(Db, fun(Tx) -> redwall:set(Tx, key, value) end).
```

- The Proxy of the database is the process of the current epoch, from
  the information that the worker of the node has.
- The StorageServer of the database is the StorageServer of the node. A
  read uses its ETS tables in the process of the caller.
- `redwall:open/1` takes the same options as before, and each option
  replaces a default. For example, `redwall:open(#{profile => ets})`
  uses the profile for the ETS StorageServer.

## Status

`redwall_status:status/1` returns the health of a cluster and its fault
tolerance in Erlang nodes ([redwall_status](../src/redwall_status.erl)).
FoundationDB reports the same two numbers for zones in the
`fault_tolerance` section of its status.

```erlang
redwall_status:status(#{
    coordinators => [{c1, 'a@host'}, {c2, 'b@host'}, {c3, 'c@host'}],
    min_workers => 2
}).
```

| Key of the result | Meaning |
|---|---|
| `generation` | The number of resets of the database after the loss of LogServers |
| `storage_engine` | The storage engine of the StorageServers that answer: `ets`, `undefined` if none answers, or `{differ, Names}` |
| `storage_durability` | `memory` if a StorageServer that stops loses its data, which holds for the engine `ets`. `disk` otherwise |
| `healthy` | The cluster has an epoch that the ClusterController has given to the workers, and every coordinator, every LogServer of the epoch and every StorageServer runs and is current |
| `fault_tolerance` | `max_node_failures_without_losing_data` and `max_node_failures_without_losing_availability`: the numbers of nodes that can stop at the same time. `safe_to_stop`: the nodes whose stop alone leaves the cluster available |
| `nodes` | The coordinator, the worker, the LogServers of the epoch and the StorageServer of each node |
| `coordinators`, `logs`, `workers`, `storages` | The numbers of processes that run |
| `problems` | The reasons for which the cluster is not healthy |

- The cluster keeps its data after the stop of a set of nodes if fewer
  than `replication` LogServers of the current epoch are stopped, and a
  current StorageServer is on another node.
- The cluster stays available if it also keeps a majority of the
  coordinators and `min_workers` registered workers.
- An operator who restarts the nodes one after another stops a node only
  if it is in `safe_to_stop`, and waits for `healthy` before the next
  node.
- Without the option `storages`, the StorageServers are the
  StorageServers of the coordinated state.
- Limitations: the function asks the processes one after another, and
  does not examine partitions.

The simulation of etx uses the function for the kills of its runs with
restarts of different nodes. See the README of etx.
