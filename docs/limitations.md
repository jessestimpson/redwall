# Limitations

- A commit waits for the time of a batch, 1 ms by default, also when no
  other transaction arrives. The time does not change with the load.
- A batch has no limit in bytes.
- The client has a part of the functions of erlfdb. `get_range/4` has
  the options `limit`, `reverse` and `snapshot`, and no fold and no
  iterator. A database with the kind `any` has no key selectors. A key
  selector in a database with the kind `binary` does not find a key at
  or above `<<255>>`. The client has no atomic operations, no
  versionstamped keys and values, no transaction options, no
  tenants and no `get_last_error/0`. It has the tuple layer, but no subspaces
  and no directory layer.
- Each watch has a process in the client and a request at one
  StorageServer. Two watches of one database on one key do not share a
  request, as they do in FoundationDB.
- A watch ends when the process that receives its message ends. The
  client does not learn that an application has dropped a future.
- Only the process that receives the message of a watch can wait for
  its future with `wait/1,2`.
- A StorageServer does not learn the stop of a client with a watch. It
  removes the request of the watch after `watch_timeout_ms`.
- The simulation with watches and a check that every watch becomes
  ready has no faults. In the runs with faults, a client waits for
  3,000 ms, and the simulation checks only a future that is ready.
- The model of the watch has one key, one watch and one StorageServer.
  It is not composed with the model of the recovery.
- A database of the client is a Proxy and a list of StorageServers that
  the caller names. redwall has no process that tells a client which
  Proxy and which StorageServers run.
- A read by message waits for its part of the 5 seconds at a
  StorageServer that is behind or cut off, before it continues with the
  next StorageServer. The client has no record of the StorageServers that
  did not answer an earlier read.
- The coordinated state has the process of each LogServer. A LogServer
  that starts again after a stop is another process, and no log system
  names it.
- Two unit tests, in
  [test/redwall_nodes_tests.erl](../test/redwall_nodes_tests.erl), use two
  Erlang nodes on one computer. One of them has a worker and a candidate
  for the ClusterController on each node. Each test starts Erlang
  distribution and
  needs `epmd`. No test cuts the connection between two Erlang nodes.
  The simulation cuts the connection between simulated nodes.
- Two processes on two nodes can register one name at the same time.
  The client raises the error 1037 when the Proxy does not answer. The
  client of FoundationDB waits for a new Proxy.
- A transaction of `create_transaction/1` keeps its ETS table until its
  process ends. erlfdb frees a transaction when no process has a
  reference to it.
- Two processes that use one transaction at the same time get no error.
- The simulation does not use `transactional/2` and `on_error/2`. Unit
  tests cover them.
- redwall has one storage engine, `ets`. With it, the StorageServer
  keeps the data in memory, and loses the data when it stops. A new
  StorageServer copies the data from another StorageServer.
  Without another StorageServer with data, it reads every record again
  from the LogServers, which is possible only while the LogServers of
  every epoch run and have discarded no record.
- A read at a version that is not a read version from a Proxy can return
  a write that a recovery discards.
- After a LogServer is stopped, the StorageServer reads the coordinated
  state every 500 ms until the coordinated state has a later epoch.
- A read of a key range returns every key of the range. A read has no
  limit of rows.
- A read copies each value from the ETS table to the StorageServer, and
  from the StorageServer to the client.
- In the simulation, a read that returns no value is accepted when a
  transaction without a known result cleared the key at any version.
- The harness of the simulation orders the StorageServers by pid. After
  about 1,048,576 processes in one Erlang VM, the VM reuses pid values,
  and a seed then does not reproduce a run. `dst/run.sh` runs each test
  in a VM of its own, which stays below that number. See
  [ROADMAP.md](../ROADMAP.md).
- The MVCC window of the StorageServer and the MVCC window of the Resolver
  are two options. No part of redwall checks that they are equal.
- In the simulation, a new StorageServer starts without data after a
  kill. The simulation kills at most 1
  LogServer of each epoch, so a LogServer with each body runs.
- A request for a read version waits until the timeout of the client
  when a message to a LogServer or its answer is lost. The Proxy does not
  send the message again.
- A Resolver that stops is not started again. The commits of its epoch
  stop until the next recovery.
- A Proxy that stops between the assignment of an LSN and the push leaves
  an LSN that no LogServer receives. Every later record of the epoch then
  waits in the LogServers until a recovery.
- A commit to a LogServer that is alive and does not answer waits until
  the timeout of the client. The Proxy does not stop in this case.
- A LogServer that has stopped must not start again with the same
  identifier, because it has lost its records.
- A candidate for the ClusterController proposes itself when the
  connection to the node of the ClusterController ends. If two candidates
  cannot connect to each other and both can connect to the coordinators,
  each election replaces the ClusterController, and each new
  ClusterController starts a recovery.
- A leader that abandons an attempt stops the Sequencer of the attempt
  when it learns the Sequencer. If that Sequencer has raised the recovery
  count, the recovery of the next attempt can fail, and the leader
  recruits again. In 50 simulation runs without faults, 2 of 270
  recoveries failed in this way.
- A new ClusterController starts a recovery, also when the roles of the
  current epoch run. FoundationDB has the same property.
- The ClusterController does not stop the process of an ended epoch.
  The process continues to run until its next commit or its next request
  for a read version.
- The ClusterController does not start StorageServers.
- The roles do not read the information of the ClusterController about
  the database. They read the process of each LogServer from the
  coordinated state.
- The client does not find the Proxy from the coordinators.
  `redwall:open/0` works on a node with a worker and a StorageServer of
  the application. On another node, a caller names the Proxy and the
  StorageServers.
- `redwall:open/0` reads the StorageServer of the node once. A database
  from before a new start of the StorageServer has the process of the
  earlier one.
- The worker and the candidate for the ClusterController of a node are
  two linked processes. They are not one process, because each of them
  is simpler alone: a worker lives as long as its node, and a candidate
  changes between candidate and leader.
- A worker and a candidate read the coordinated state every `poll_ms`. A
  process of FoundationDB waits for a change with one request.
- The application starts the coordinator of a node, and does not form
  the peace cluster. A caller forms it once with
  `redwall_coordinator:form/1`.
- redwall has no function that replaces a coordinator whose directory
  is lost, and none that changes the coordinators of a cluster.
- The recovery waits up to `lock_timeout_ms` (default 1,000 ms) for an old
  LogServer that does not answer.
- The tests of the recovery stop LogServers between steps. No test stops a
  process during a step.
- A LogServer keeps the records in memory, with every storage engine. A
  LogServer that stops loses the records. A cluster with the storage
  engine `ets` therefore starts without data after the stop of every
  node.
- The interface of a storage engine is that of the engine `ets`: the
  engine keeps the versions of the MVCC window. An engine on a disk
  needs a StorageServer that keeps these versions in memory and reads
  the engine at its start. Both are not built.
- The coordinated state does not record the storage engine. A
  StorageServer with another engine than the other StorageServers of
  its cluster starts. The status reports the problem
  `storage_engines_differ`.
- A LogServer discards a record only after every StorageServer of the
  list of the coordinated state has stored it, and only if the list has
  `pop_storages` StorageServers. Otherwise the memory of a LogServer
  grows with every record. See [Pop](log-server.md#pop).
- The number of records that wait in a LogServer has no limit.
- The model of the recovery, the model of the Resolver and the model of
  the watch are separate. No model has two of the three.
- The models have no keys and values in the StorageServer, no tags, no
  clock and no election of a ClusterController. The model of the Resolver
  has a key range as a set of keys, without the bounds.
- The Recovery configs with 3 epochs take about 4 hours. They are not run
  for every change. See "When to run TLC" in
  [formal/README.md](../formal/README.md).
- The models are checked with 3 LogServers, a replication degree of 2, at
  most 3 epochs and 7 versions, and for the Resolver 2 keys and 3
  transactions.
- redwall does not send a message again. In the simulation, a lost
  message to a LogServer stops the commits of the epoch until the next
  recovery.
