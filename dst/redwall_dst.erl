%% This module is the eta harness for the transaction system of redwall.
%% The harness starts the system, runs a workload of commits, recoveries and
%% kills against it, and checks invariants after every step.
%%
%% == Simulated system ==
%%
%% <ul>
%% <li>The coordinated state is one `redwall_cstate_local' process, or a
%%   peace cluster (see "Runs with a peace cluster"). The process applies one value at a time,
%%   which is the register with atomic operations of
%%   `formal/Recovery.tla'.</li>
%% <li>The process of an epoch, a recovery and a StorageServer read the
%%   process of each LogServer from the coordinated state. Two simulated
%%   nodes connect with the first message between them.</li>
%% <li>A `recover' operation starts the process of a new epoch
%%   (`redwall_epoch'), which does the work of the Sequencer, of the
%%   Resolver and of the Proxy. This is the work of a ClusterController. In these runs the harness does
%%   this work at times that the seed selects, so two recoveries overlap
%%   more often than with a ClusterController.</li>
%% <li>A recovery recruits 3 LogServers with a replication degree of
%%   2.</li>
%% <li>A run has 1, 2 or 3 StorageServers. Each one sends each request
%%   to a LogServer again every 1 s of the virtual clock.</li>
%% <li>The run has the simulated network of eta. The coordinated state,
%%   the StorageServer, each Sequencer, each Proxy, each Resolver and each
%%   LogServer run on a simulated node of their own. All clients share one node. The network policy of the
%%   run can lose or delay a message between two nodes.</li>
%% <li>A LogServer starts on its own node, through `erpc' from the node
%%   of the process of the epoch.</li>
%% </ul>
%%
%% == Runs with a peace cluster ==
%%
%% With the config `cstate => peace', the coordinated state is a peace
%% cluster in the place of the `redwall_cstate_local' process:
%%
%% <ul>
%% <li>The cluster has 3 servers, each on a simulated node of its own,
%%   with majority quorums, an election time limit of 50 ms and a
%%   heartbeat of 10 ms. peace is compiled with the parse transform of
%%   eta in the `dst' profile.</li>
%% <li>The first `recover' operation starts the servers, and a process
%%   that forms the cluster (`peace:form_cluster/3').</li>
%% <li>The machine of the servers is `redwall_dst_machine', which applies
%%   the values with `redwall_cstate' and records the state of the
%%   highest index that a server has applied. The harness reads that
%%   state for the invariants.</li>
%% <li>Each server has the configuration of `redwall_coordinator': it
%%   keeps its data in the write-ahead log of peace, on the simulated
%%   disk of eta (`disk => true' of the run).</li>
%% <li>`{kill_coordinator, R}' kills the node of one server that runs, or
%%   of every server that runs in 1 operation of 4, after the cluster has
%%   formed. The kill is a crash of the disk of the node. The next
%%   `recover' operation starts each killed server again with its
%%   data.</li>
%% <li>The invariant `coordinators_agree': no two servers have two states
%%   of the coordinated state for one index of the log of peace.</li>
%% <li>The planted defect `unsynced_coordinators': a server never syncs
%%   its files (`redwall_dst_nosync_io').</li>
%% <li>`{partition, R1, R2}' can cut a node of a server off. A cut can
%%   leave the cluster without a majority until the next `heal'.</li>
%% <li>A call to the cluster can end without an answer after peace has
%%   decided its value. The `redwall_cstate_local' process has this case
%%   only for a lost answer.</li>
%% </ul>
%%
%% == Runs with ClusterControllers ==
%%
%% With the config key `controller', ClusterControllers recruit the
%% Sequencers:
%%
%% <ul>
%% <li>The first `recover' operation starts 8 worker nodes: 4 of the class
%%   `stateless', of which 3 have a candidate for the ClusterController,
%%   and 4 of the class `log'. Each worker has `poll_ms' of 200 ms. A
%%   ClusterController recruits with at least 3 workers.</li>
%% <li>Each later `recover' operation starts a new worker node for each
%%   killed worker node, and asks the ClusterController for a recovery
%%   (`redwall_cluster_controller:force_recovery/1').</li>
%% <li>`kill_log', `kill_sequencer', `kill_proxy' and `kill_resolver' kill
%%   the worker node of one role of that kind that runs. The kill stops
%%   every role on the node, and the candidate if the node has one.</li>
%% <li>The harness kills a worker node only if no epoch then has 2 killed
%%   LogServers.</li>
%% <li>The ClusterController recruits again after the failure of a role,
%%   and a candidate wins an election after the failure of the
%%   leader.</li>
%% <li>After the last operation, the harness removes every cut, replaces
%%   every killed worker node, and stops the loss of messages
%%   (`repair/2'). The run continues until the checks of `check_final/2'
%%   hold, for at most the settle steps of the run. The suite uses 20,000
%%   settle steps.</li>
%% <li>With the config `watches', the workload of the run has watches.
%%   With the config `late_watches', the harness also starts watches
%%   after the last operation. See "Runs with late watches".</li>
%% </ul>
%%
%% The invariants of the other runs hold in these runs. These runs have
%% one more invariant, `one_leader': no two ClusterController processes
%% are the leader of one term. `check_final/2' checks at the end of a run:
%%
%% <ul>
%% <li>`leader' - the ClusterController of the coordinated state runs and
%%   is the leader.</li>
%% <li>`stale_leader' - no other process is a leader.</li>
%% <li>`epoch' - the leader has the current epoch of the coordinated
%%   state, and the process of the epoch runs.</li>
%% <li>`worker_db_info' - each worker that runs has the information of
%%   the leader about the database.</li>
%% <li>`late_commit' and `watch_fires' - in a run with the config
%%   `late_watches'. See "Runs with late watches".</li>
%% </ul>
%%
%% == Runs with the pop ==
%%
%% With the config keys `controller' and `pop', the LogServers discard
%% records (see "Pop" in `redwall_cluster_controller'):
%%
%% <ul>
%% <li>The harness records each StorageServer in the list of the
%%   coordinated state, under the name of its place. A new StorageServer
%%   of a place replaces the killed one in the list, as the StorageServer
%%   of a node that starts again does.</li>
%% <li>The ClusterController sends a pop when the list has 2
%%   StorageServers or more. A run with 1 StorageServer has no pop.</li>
%% <li>The StorageServer of the place 3 is not in the list. The
%%   LogServers can discard a record that it has not stored. It then
%%   copies the data from another StorageServer.</li>
%% <li>A StorageServer of the place 3 that starts after a kill finds no
%%   peer at its first question, and reads the log from the start. A
%%   LogServer answers that it has discarded the first records, and the
%%   StorageServer then copies from a peer.</li>
%% <li>The harness kills a StorageServer only if another StorageServer
%%   runs with a version above 0 that is at or above the version of the
%%   first one. A StorageServer keeps its data in memory, so the
%%   discarded records are lost when every StorageServer with them
%%   stops.</li>
%% <li>The harness keeps the LSN of the record of each client from the
%%   records of the LogServers at each check, because a LogServer does
%%   not keep a discarded record.</li>
%% <li>In a run with the config `watch_clears', the harness also keeps
%%   the changes of the keys of the watches from the records of the
%%   LogServers at each check. See "Runs with clears of the keys of the
%%   watches".</li>
%% </ul>
%%
%% == Runs with watches ==
%%
%% With the config key `watches', the workload has watches (see "Watches"
%% in `redwall'). The value of the key is `true' or `live'.
%%
%% <ul>
%% <li>1 operation in 4 is `{watch, R1, R2}', in the place of a
%%   transaction. A client creates a watch on one of 2 keys, which are
%%   not the keys of the transactions. 2 clients in 3 set the key to the
%%   value `{watch, C}' in the transaction of the watch. The other
%%   clients read the key. Without the config `watch_clears', no client
%%   clears a key of a watch, and each client writes its own value, so
%%   the value of a key never changes back.</li>
%% <li>The client waits for the future of its watch. When the future is
%%   ready, the client reads the key in a new transaction.</li>
%% <li>The invariant `watch_change': the read after a ready future
%%   returns another value than the value of the watch. A run with the
%%   config `watch_clears' has another statement of `watch_change'.</li>
%% <li>The invariant `watch_error': the future of a watch whose commit
%%   failed has the error of the commit, and the future of a watch whose
%%   commit succeeded has no error.</li>
%% <li>With `watches => true', a StorageServer answers `timed_out' to a
%%   watch after 300 ms, a request of a watch has a time limit of 1,000
%%   ms, and a client waits for its future for 3,000 ms.</li>
%% <li>With `watches => live', a StorageServer and a request of a watch
%%   have no time limit, and a client waits without a limit. A watch then
%%   becomes ready only after the answer of a StorageServer that the
%%   value differs. `check_final/2' checks `watch_fires': the future of
%%   every watch is ready, if an acknowledged transaction set the key of
%%   the watch at a commit version above the version of the watch. A run
%%   with the config `watch_clears' has another statement of
%%   `watch_fires'. The run continues until the check holds, for at most
%%   the settle steps of the run. This config is for runs without faults:
%%   a lost message is not sent again.</li>
%% <li>The planted defect `watch_no_trigger': a StorageServer does not
%%   read the key of a watch when it applies a record. `watch_fires'
%%   finds it.</li>
%% <li>The planted defect `watch_no_verify': the client does not read the
%%   key after the answer of a StorageServer. A StorageServer answers
%%   for a record that a recovery discards, and `watch_change' finds the
%%   ready future.</li>
%% </ul>
%%
%% == Runs with clears of the keys of the watches ==
%%
%% With the config `watch_clears => true', in a run with the config
%% `watches', clients clear the keys of the watches:
%%
%% <ul>
%% <li>1 client in 3 with a watch sets the key to the value `{watch, C}',
%%   1 client in 3 reads the key, and 1 client in 3 clears the key. Each
%%   client creates the watch in the same transaction. The value of the
%%   watch of a client that clears the key is `not_found'.</li>
%% <li>The value of a key changes back to `not_found': one client sets
%%   the key, and another client clears the key. A client whose watch has
%%   the value `not_found' can therefore read `not_found' after the
%%   future of the watch is ready. The future of such a watch can also
%%   stay not ready after a set of the key, if another transaction clears
%%   the key before the client has read the key. See the guarantees in
%%   "Watches" in `redwall'.</li>
%% <li>The harness keeps the changes of the 2 keys of the watches from
%%   the records of the LogServers at each check
%%   (`remember_watch_records/2'). A kept change has the LSN of the
%%   record, the epoch of the LogServer, and the value of the key after
%%   the record. The value of the key after a record is the result of the
%%   mutations of the transactions of the record, in their order.</li>
%% <li>The harness does not use a kept change of a record that a recovery
%%   discarded (`watch_history/3'). The harness treats a record as
%%   discarded in two cases. In the first case, the record is on a
%%   LogServer of an ended epoch, and its LSN is above the end version of
%%   that epoch. In the second case, the LSN is above the end version of
%%   an ended epoch and below the first version of the next epoch.</li>
%% <li>The value of a key at a version `X' is the value after the kept
%%   change with the highest LSN at or below `X' that the harness uses.
%%   Without such a change, the value is `not_found'.</li>
%% <li>The invariant `watch_change' in these runs: when the future of a
%%   watch is ready, the read after it returns another value than the
%%   value of the watch, or the key has another value than the value of
%%   the watch at a version `X' with `Version =< X' and
%%   `X =< ReadVersion'. `Version' is the version of the watch, and
%%   `ReadVersion' is the read version of the read. The client records the
%%   value and the read version of the read, and the harness decides
%%   (`watch_rereads/2').</li>
%% <li>`watch_fires' in these runs with `watches => live': at the end of
%%   the run, two statements hold. First, each StorageServer that runs
%%   has a version at or above the commit version of every watch whose
%%   transaction set or cleared the key. Second, the future of a watch is
%%   ready, if a StorageServer that runs has a version at or above the
%%   version of the watch, and the value of the key at the version of that
%%   StorageServer differs from the value of the watch
%%   (`watch_fires/2').</li>
%% <li>The second statement follows the StorageServer and the client. A
%%   StorageServer answers a watch when the value of the key after an
%%   applied record differs from the value of the watch. The client then
%%   reads the key. If the read returns the value of the watch, the
%%   client sends the watch again. A watch whose future is not ready is
%%   therefore a request in a StorageServer whose value of the key is the
%%   value of the watch. The first statement prevents a run that ends
%%   before the StorageServers have applied the changes.</li>
%% <li>The statement of `watch_fires' of the runs without clears does not
%%   hold in these runs. The future of a watch with the value `not_found'
%%   can stay not ready after an acknowledged set of the key.</li>
%% <li>`watch_fires' finds the planted defect `watch_no_trigger' in these
%%   runs: a StorageServer has another value of the key than the value of
%%   a watch whose future is not ready. `watch_change' finds the planted
%%   defect `watch_no_verify' in these runs with faults: the read returns
%%   the value of the watch, and the key has no other value up to the
%%   read version.</li>
%% <li>The statistics `watches_ready_set', `watches_ready_read' and
%%   `watches_ready_clear' count the ready futures of each kind of
%%   transaction. `watch_keys_changed_back' counts the changes that gave a
%%   key the value that it had before the previous change.
%%   `watches_changed_back' counts the reads after a ready future that
%%   returned the value of the watch. `watches_waiting_after_change'
%%   counts the watches whose future is not ready at the end of the run
%%   after a change of the key to another value.</li>
%% </ul>
%%
%% == Runs with late watches ==
%%
%% With the config `late_watches => true', the harness starts watches
%% after it has stopped the faults. The config needs the configs
%% `controller', `last_op', `watches => true' and `watch_clears => true'.
%% A run with late watches can have the config `pop'.
%%
%% <ul>
%% <li>Until the last operation, the run is a run with ClusterControllers
%%   and with the workload of `watches => true' and of `watch_clears': a
%%   request of a watch has a time limit of 1,000 ms, and a client waits
%%   for its future for 3,000 ms.</li>
%% <li>A StorageServer of these runs has no time limit for a watch, and
%%   does not answer `timed_out'. A StorageServer takes the time limit at
%%   its start. With a time limit, the client reads the key after each
%%   answer `timed_out', and the future of a late watch becomes ready
%%   without an answer of a StorageServer that the value differs. No
%%   check then finds the planted defect `watch_no_trigger'.</li>
%% <li>After the last operation, `repair/2' also asks the
%%   ClusterControllers for a recovery. The epoch of the time of the
%%   faults can have a LogServer without a record that the network lost.
%%   Such an epoch gives read versions and commits no transaction.</li>
%% <li>`repair/2' then starts 4 clients with a late watch
%%   (`redwall_dst_client:late_watch/3'). 2 of them use the first key of
%%   the watches, and 2 use the second key. The seed selects the kind of
%%   each transaction: a set, a read or a clear of the key. A late client
%%   has no time limit for a request of a watch and for its future. It
%%   runs its transaction again after an error, until the commit
%%   succeeds.</li>
%% <li>`repair/2' also starts 2 clients with a late change
%%   (`redwall_dst_client:late_change/4'), one for each key. A client
%%   with a late change waits until the 4 late watches have a committed
%%   transaction. It then sets its key to its own value `{watch, C}',
%%   and runs the transaction again after an error. A late change
%%   therefore has a commit version above the version of each late watch
%%   of its key, and another value than the value of each late watch.</li>
%% <li>`check_final/2' checks `late_commit': each of the 6 late clients
%%   has a committed transaction.</li>
%% <li>`check_final/2' checks `watch_fires' for the late watches, with
%%   the two statements of "Runs with clears of the keys of the watches".
%%   First, each StorageServer that runs has a version at or above the
%%   commit version of every late change, and of every late watch whose
%%   transaction set or cleared the key. Second, the future of a late
%%   watch is ready, if a StorageServer that runs has a version at or
%%   above the version of the watch, and the value of the key at the
%%   version of that StorageServer differs from the value of the
%%   watch.</li>
%% <li>The run continues until the checks of `check_final/2' hold, for at
%%   most the settle steps of the run.</li>
%% <li>`watch_fires' does not check a watch from the time of the faults.
%%   A client of that time stops to wait after 3,000 ms.</li>
%% <li>`watch_change' and `watch_error' hold for every watch of the run,
%%   also for the late watches.</li>
%% <li>`watch_fires' finds the planted defect `watch_no_trigger' in these
%%   runs: the future of a late watch is not ready after a late
%%   change.</li>
%% <li>The statistics `late_watches', `late_changes' and
%%   `late_watches_ready' count the late watches and the late changes
%%   with a committed transaction, and the ready futures of the late
%%   watches. `late_changes_above' counts the late changes with a commit
%%   version above the version of every late watch of their key.
%%   `late_retries' counts the failed attempts of the late clients.</li>
%% </ul>
%%
%% == Runs with mixed keys ==
%%
%% With the config `keys => mixed', the 4 keys of the workload are terms
%% of 4 types. The default is `keys => packed': the 4 keys are packed
%% tuples, which are binaries.
%%
%% <ul>
%% <li>The 4 keys are, in the order of Erlang terms: the number `1', the
%%   atom `k', the tuple `{k, 3}' and the binary `<<"k4">>'. The upper
%%   bound of the read ranges and of the cleared ranges is the binary
%%   `<<"k5">>'.</li>
%% <li>The number key has the 2 forms `1' and `1.0', and the tuple key
%%   has the 2 forms `{k, 3}' and `{k, 3.0}'. The 2 forms of a key are
%%   equal (`==') and not identical (`=:='). redwall treats them as one
%%   key (see "Equal keys" in `redwall_storage'). The random number of a
%%   client selects one form of each of the 2 keys. The client uses these
%%   forms for its reads, its writes and the bounds of its ranges. Two
%%   clients then use 2 forms of one key.</li>
%% <li>A StorageServer returns the key of the last write that it has
%%   applied. A read of a range can therefore return a key in another
%%   form than the form of the client, and 2 reads at one read version
%%   can return one key in 2 forms.</li>
%% <li>The harness and the client compare the canonical forms of keys
%%   (`redwall_dst_client:canon/1'): the canonical form of `1.0' is `1'.
%%   A client records the canonical forms of its read keys, of its
%%   written keys and of its cleared keys in the table of the run, and
%%   the canonical forms of the keys of the rows that it read. The
%%   harness uses the canonical form of each key of a StorageServer. The
%%   invariants `no_conflict', `read_value', `replicas_agree' and
%%   `storage_rollback' therefore treat `1' and `1.0' as one key. A client
%%   compares the canonical forms of two results for `select_agrees',
%%   `range_options_agree' and `own_writes'.</li>
%% <li>For `own_writes', a client also reads each key that it set or
%%   cleared in the other form of the key.</li>
%% <li>A run with mixed keys does not have the parts of the workload that
%%   need packed keys or binary keys:
%%   <ul>
%%   <li>No client calls `redwall:get_mapped_range/4', because a mapper
%%     reads packed keys. A client reads every read range with
%%     `get_range/3'. The invariant `mapped_range_agrees' has no read to
%%     check, and `own_writes' does not check the error 2039.</li>
%%   <li>No client calls `redwall:get_key/2', because key selectors need
%%     a database with the kind `list' or `binary'. `range_options_agree'
%%     checks the options `reverse' and `limit' of `get_range/4' in a
%%     database with the kind `any'.</li>
%%   <li>The run has no watches. The config `watches' with
%%     `keys => mixed' is an error.</li>
%%   </ul></li>
%% <li>The statistics `integer_key_reads', `float_key_reads',
%%   `integer_key_writes' and `float_key_writes' count the transactions
%%   that read the number key with `get/2', and the acknowledged
%%   transactions that set it, for each form. `other_form_rows' counts
%%   the rows of the range reads with a key in another form than the form
%%   of the client.</li>
%% <li>The planted defect `no_equal_keys': the Resolver uses the row of a
%%   bound only if the bound of the row and the bound are identical
%%   (`equal_keys => false' of `redwall_resolver:resolve/4'). A read of
%%   one form of a key then has no conflict with a write of the other
%%   form. `no_conflict' finds the defect in a run with mixed keys. A run
%%   with packed keys has no two keys that are equal and not identical,
%%   so no invariant finds the defect in such a run.</li>
%% </ul>
%%
%% == Workload ==
%%
%% <ul>
%% <li>`recover': start the process of a new epoch.</li>
%% <li>`{transaction, R1, R2}': a client runs one transaction. It gets a
%%   read version from one Proxy, and commits through a Proxy that can be
%%   another one. Between the two steps, the client reads its read keys
%%   from the StorageServer at the read version. The StorageServer has an
%%   MVCC window of 100,000 versions in the runs with an odd seed, and of
%%   5,000,000 versions in the other runs. One client in three then
%%   waits, and commits through the Proxy of the highest epoch that
%%   runs. The client uses the Proxy of the highest epoch in 3 of 4
%%   cases, and any Proxy that runs in the others. A Proxy can belong to an
%%   epoch that has ended. The transaction reads and writes some of 4
%%   keys. 2 transactions in 7 also read a key range. 2 transactions in
%%   11 clear a key range before they write their keys. The 4 keys are
%%   packed tuples (`redwall_tuple:pack/1'), or terms of 4 types with the
%%   config `keys => mixed' (see "Runs with mixed keys").</li>
%% <li>`{kill_log, R}': kill one LogServer of the current epoch. The
%%   harness kills at most 1 LogServer of each epoch, because the design
%%   tolerates the loss of k - 1 LogServers of an epoch, and a LogServer of
%%   redwall keeps its records in memory.</li>
%% <li>`{kill_sequencer, R}': kill the process of one epoch. The process
%%   can be in the middle of its recovery.</li>
%% <li>`{kill_proxy, R}': kill the process of one epoch whose recovery
%%   has ended.</li>
%% <li>`{kill_resolver, R}': kill the process of one epoch whose recovery
%%   has ended, at most once for each epoch. The three operations are
%%   from the time when the Sequencer, the Proxy and the Resolver were
%%   three processes.</li>
%% <li>`{kill_storage, R}': kill one StorageServer, and start a new
%%   StorageServer without data on a new node. The new StorageServer
%%   copies the data from another StorageServer of the run that has data,
%%   and otherwise reads every record from the LogServers of every epoch
%%   (see "Copy from another StorageServer" in `redwall_storage'). In a run with
%%   several StorageServers, the new StorageServer starts at once after 1
%%   kill in 2, and with the next `recover' operation after the other
%%   kills. A run has 1, 2 or 3
%%   StorageServers, each on a node of its own. The number depends on the
%%   seed. A client reads from the StorageServers of the run through the
%%   client library, which continues with the next StorageServer when one
%%   does not answer. 1 client in 2 runs on the node of a StorageServer
%%   and reads the ETS table of that StorageServer. A kill of the node
%%   also stops these clients. The other clients run on the node
%%   `clients' and read by message. 1 client in 5 reads by message in
%%   every case.</li>
%% <li>`{partition, R1, R2}': cut the connection between two nodes. Each
%%   side receives `noconnection' for its monitors on the other side.</li>
%% <li>`heal': remove every cut.</li>
%% </ul>
%%
%% A kill stops every process on the node of the target
%% (`eta_net:kill_node/1').
%%
%% `R' is a random number that selects the target when the operation runs.
%% An operation contains no pid, so that a trace can be replayed.
%%
%% == Invariants ==
%%
%% The harness checks these invariants after every step. The first three
%% are invariants of the model.
%%
%% <ul>
%% <li>`durable' - when an epoch has ended, every LSN that a client
%%   received as committed in that epoch is at or below the Recovery
%%   Version that ended the epoch. This is `Durable' of the model.</li>
%% <li>`unique_versions' - no LSN is acknowledged in two epochs, and no
%%   pair of an LSN and an index in a batch is acknowledged to two
%%   clients. This is `UniqueVersions' of the model.</li>
%% <li>`no_loss' - no recovery ends with `missing_body'. This is `NoLoss'
%%   of the model.</li>
%% <li>`body_available' - the body of every acknowledged record is on a
%%   LogServer of its epoch that runs, or the record is at or below the
%%   popped version of such a LogServer.</li>
%% <li>`copied' - when a recovery ends an epoch, every LogServer of the new
%%   epoch that runs has the body of every acknowledged record of the
%%   ended epoch above the previous epoch's end version and above the
%%   popped version of the LogServer.</li>
%% <li>`popped_stored' - a StorageServer that runs has a version at or
%%   above the popped version of every LogServer that runs.</li>
%% <li>`no_conflict' - no acknowledged transaction read a key, alone or in
%%   a key range, that another
%%   acknowledged transaction wrote at a commit version above the read
%%   version of the first transaction and below its commit version, or
%%   at its commit version with a lower index in the batch. This is the
%%   guarantee of the Resolver.</li>
%% <li>`read_version_order' - a read version is at or above the commit
%%   version of every transaction that was acknowledged before the client
%%   asked for the read version, in any epoch. The harness checks every
%%   read version that a client received, also when the commit failed. A Proxy of an ended epoch
%%   must not give a read version.</li>
%% <li>`read_value' - a read of a key at a read version returns the write
%%   of the acknowledged transaction with the highest commit version at or
%%   below the read version, and with the highest index among the
%%   transactions of that version, or a write with a higher commit version
%%   at or below the read version whose client received no answer. No
%%   acknowledged transaction wrote or cleared the key between the
%%   returned write and the read version. The read returns `not_found'
%%   only in three cases: no acknowledged transaction wrote the key at or
%%   below the read version, the last acknowledged transaction that
%%   changed the key cleared it, or a transaction without a known result
%%   cleared the key.</li>
%% <li>`storage_rollback' - when the StorageServer has a version of an
%%   epoch, it has no write between the end version of an earlier epoch
%%   and the first version of the epoch after that one. A recovery
%%   discarded every record in that range.</li>
%% <li>`replicas_agree' - two StorageServers have the same writes for each
%%   key, for the versions that both have applied and that are above the
%%   oldest version of both. The writes that a recovery discarded are not
%%   compared, because `storage_rollback' covers them.</li>
%% <li>`select_agrees' - a `redwall:select/4' of a client returned the
%%   result of `ets:match_spec_run/2' on the result of `get_range/3' at
%%   the same read version. A client with a read range evaluates two
%%   match specs on the range.</li>
%% <li>`mapped_range_agrees' - a `redwall:get_mapped_range/4' of a client
%%   returned the rows of `get_range/3' at the same read version, each
%%   with the result of its second read. A client with a read range reads
%%   the range with two mappers: one with a second read of a key, and one
%%   with a second read of a prefix.</li>
%%   1 read range in 4 of a transaction is read with
%%   `get_mapped_range/4' only, so that `no_conflict' checks the read
%%   conflict ranges of a mapped range read. Each read with a limit
%%   returns the first rows of the read without a limit.</li>
%% <li>`range_options_agree' - a `redwall:get_range/4' of a client with
%%   the options `reverse' and `limit' returned the rows of `get_range/3'
%%   at the same read version in the reverse order, the first rows, and
%%   the last rows. `redwall:get_key/2' returned the first key at or
%%   above the start of the range, and the last key below its end.</li>
%% <li>`own_writes' - before the commit, the reads of a transaction
%%   return its sets and its clears: `get/2', `get_range/3' and
%%   `select/4' of a snapshot. `get_mapped_range/4' raises 2039. After a
%%   failed commit, `on_error/2' leaves a transaction without a read
%%   version and without a mutation.</li>
%% <li>`watch_change' and `watch_error' - in a run with watches. See
%%   "Runs with watches". A run with the config `watch_clears' has
%%   another statement of `watch_change'. See "Runs with clears of the
%%   keys of the watches".</li>
%% <li>`kcv_below_dv' - in each epoch, the Known Committed Version of
%%   every LogServer that runs is at or below the Durable Version of every
%%   LogServer that runs.</li>
%% <li>`chain' - in each LogServer, the previous LSN of each record is the
%%   LSN of the record before it.</li>
%% </ul>
%%
%% == Planted defects ==
%%
%% The config key `mode' selects a defect:
%%
%% <ul>
%% <li>`ack_replicas' - a Proxy answers a client after the 2 LogServers
%%   with the body have answered. See `RecoveryAckReplicasOnly' in
%%   `formal/README.md'.</li>
%% <li>`no_cas' - the coordinated state accepts a write from a Sequencer
%%   whose epoch is not the recovery count. See `RecoveryNoCas'.</li>
%% <li>`no_copy' - a recovery copies no record to the new LogServers.</li>
%% <li>`no_wait' - a StorageServer answers a read at a version that it has
%%   not applied.</li>
%% <li>`one_no_clear' - the second StorageServer of a run does not apply a
%%   clear.</li>
%% <li>`no_elect_cas' - the coordinated state accepts the proposal of a
%%   candidate with any term. This defect violates `one_leader'.</li>
%% <li>`no_monitor_roles' - a ClusterController does not recruit after
%%   the failure of a role. `check_final/2' finds a run that ends without
%%   an epoch.</li>
%% <li>`no_step_down' - a ClusterController stays the leader when the
%%   coordinated state has another term. `check_final/2' finds a run that
%%   ends with two leaders.</li>
%% <li>`no_check' - the Resolver admits every transaction.</li>
%% <li>`no_range_check' - the Resolver does not check the read ranges of a
%%   transaction.</li>
%% <li>`no_batch_check' - the Resolver does not check a transaction
%%   against the earlier transactions of its batch.</li>
%% <li>`no_gap' - the first commit version of an epoch is the Recovery
%%   Version plus 1. The Resolver of the new epoch then admits a
%%   transaction with a read version of the previous epoch.</li>
%% <li>`no_report' - a Proxy does not report a committed LSN to the
%%   Sequencer.</li>
%% <li>`no_confirm' - a Proxy gives a read version without the
%%   confirmation that its epoch is live.</li>
%% <li>`no_rollback' - the StorageServer keeps the writes above the end
%%   version of an epoch.</li>
%% <li>`no_write_range' - the Resolver does not record the write ranges of
%%   a transaction, so a clear of a key range gives no conflict.</li>
%% <li>`no_clear' - the StorageServer does not apply a clear.</li>
%% <li>`no_window_check' - the StorageServer answers a read below its
%%   oldest version, after it has removed older versions.</li>
%% <li>`no_push_order' - a LogServer stores a record whose previous
%%   record has not arrived. A Proxy has two records on the way at the
%%   same time when it closes a batch while an earlier batch is in
%%   progress: with `batch_depth' above 1, or when the time of the batch
%%   ends. `chain' finds the record.</li>
%% <li>`no_copy_rows' - a StorageServer that copies from another
%%   StorageServer stores no row of the copy. It then has the version of
%%   the copy without its data.</li>
%% <li>`watch_no_trigger' and `watch_no_verify' - see "Runs with
%%   watches", "Runs with clears of the keys of the watches" and "Runs
%%   with late watches".</li>
%% <li>`no_equal_keys' - the Resolver does not treat two keys that are
%%   equal and not identical as one key. See "Runs with mixed
%%   keys".</li>
%% <li>`pop_no_limit' - the pop version of the ClusterController is the
%%   highest version of a StorageServer, and a LogServer discards the
%%   records without the limit of its Known Committed Version. A recovery
%%   then cannot copy a record that it must copy.</li>
%% </ul>
%%
%% == Limitations ==
%%
%% <ul>
%% <li>Without the config `cstate => peace', the coordinated state is one
%%   process that is never killed.</li>
%% <li>The peace cluster has no change of its members in a run, and no
%%   server is replaced.</li>
%% <li>A killed process does not start again. A LogServer has no disk.</li>
%% <li>The application start and `redwall:open/0' are not in the run. A
%%   client names its Proxy and its StorageServers.</li>
%% <li>A client does not run a transaction again after an error. It calls
%%   `redwall:on_error/2' and ends.</li>
%% <li>redwall does not send a message again. A lost message to a
%%   LogServer stops the commits of the epoch until the next recovery.
%%   FoundationDB runs on TCP, and treats a broken connection as a failure
%%   of the process.</li>
%% <li>The virtual clock advances only when no process can run. The
%%   time-outs of calls and the time limit of the lock fire only then.</li>
%% <li>A run with mixed keys has no mapped range read, no key selector
%%   and no watch. See "Runs with mixed keys".</li>
%% <li>In a run with mixed keys, the keys with 2 forms are the number
%%   `1' and the tuple `{k, 3}'. The workload has no list key and no map
%%   key with 2 forms.</li>
%% <li>A run with late watches has no answer `timed_out' of a
%%   StorageServer to a watch, because its StorageServers have no time
%%   limit for a watch.</li>
%% <li>In a run with faults, `watch_fires' checks the late watches only.
%%   No check requires the future of a watch from the time of the faults
%%   to become ready.</li>
%% <li>The late watches and the late changes are on the node `clients'.
%%   A run has 4 late watches and 2 late changes. A late change is a set
%%   of the key. No late change is a clear.</li>
%% <li>In a run with clears of the keys of the watches, the harness does
%%   not decide `watch_change' for a ready future whose client could not
%%   read the key in 10 attempts.</li>
%% <li>`watch_change' with clears can use a record that a recovery
%%   discarded, in one case: the ended epoch has the end version 0, and a
%%   LogServer of a recovery that did not write the coordinated state has
%%   the record. The check then accepts a read that it should not accept.
%%   The check reports no violation for this reason.</li>
%% </ul>
-module(redwall_dst).

-behaviour(eta_harness).

-export([init/2, processes/1, generate/2, execute/2, check/1, check_final/2, settle_done/1, terminate/1]).
-export([stats/0, reset_stats/0]).
%% For `redwall_dst_trace'.
-export([cstate/1, logs/1]).

-define(STATS, redwall_dst_stats).

%% The gap between the Recovery Version and the first commit version of an
%% epoch: the default of `max_versions_in_flight'.
-define(EPOCH_GAP, 100000000).

%% The numbers of the late watches and of the late changes of a run with
%% the config `late_watches'. See "Runs with late watches".
-define(LATE_WATCHES, 4).
-define(LATE_CHANGES, 2).

%% ---------------------------------------------------------------------------
%% eta_harness
%% ---------------------------------------------------------------------------

init(Seed, Config) ->
    Mode = maps:get(mode, Config, correct),
    %% The MVCC window of the StorageServer is 100,000 versions in the runs
    %% with an odd seed: the versions of 100 ms of the virtual clock. In
    %% the other runs it is the default of 5,000,000 versions.
    Window =
        case Seed rem 2 of
            1 -> 100000;
            0 -> 5000000
        end,
    %% The modules in dst/ are not part of the redwall application, so
    %% `preload => [redwall]' does not load them.
    {module, _} = code:ensure_loaded(redwall_dst_client),
    Tab = ets:new(redwall_dst, [public, ordered_set]),
    %% Whether the LogServers discard records. See "Runs with the pop".
    _ = maps:get(pop, Config, false) andalso ets:insert(Tab, {pop, true}),
    %% Whether the workload has watches: `false', `true' or `live'. See
    %% "Runs with watches".
    Watches = maps:get(watches, Config, false),
    _ = Watches =/= false andalso ets:insert(Tab, [{watches, Watches}, {mode, Mode}]),
    %% Whether clients with a watch clear the key of the watch. See "Runs
    %% with clears of the keys of the watches".
    WatchClears = maps:get(watch_clears, Config, false),
    true = not WatchClears orelse Watches =/= false,
    _ = WatchClears andalso ets:insert(Tab, {watch_clears, true}),
    %% Whether the harness starts late watches after the last operation.
    %% See "Runs with late watches".
    Late = maps:get(late_watches, Config, false),
    true = not Late orelse (Watches =:= true andalso WatchClears andalso maps:get(controller, Config, false) andalso is_integer(maps:get(last_op, Config, undefined))),
    _ = Late andalso ets:insert(Tab, {late_watches, true}),
    %% The keys of the workload: `packed' or `mixed'. See "Runs with mixed
    %% keys". A run with mixed keys has no watches, because a read range
    %% of mixed keys contains the keys of the watches.
    Keys = maps:get(keys, Config, packed),
    true = Keys =:= packed orelse (Keys =:= mixed andalso Watches =:= false),
    _ = Keys =:= mixed andalso ets:insert(Tab, {keys, mixed}),
    %% An ETS table of the caller for trace validation, of the kind
    %% `ordered_set'. See `redwall_dst_trace'.
    _ = [ets:insert(Tab, {trace, Table}) || Table <- [maps:get(trace, Config, undefined)], Table =/= undefined],
    Cas = #{cas => Mode =/= no_cas, elect_cas => Mode =/= no_elect_cas},
    %% The topology is declared before any process creates a monitor.
    CState =
        case maps:get(cstate, Config, local) of
            local ->
                {ok, Local} = redwall_cstate_local:start(Cas),
                ok = eta_net:place(coordinators, [Local]),
                true = ets:insert(Tab, {{node, coordinators}, true}),
                {local, Local};
            peace ->
                %% A peace cluster of 3 servers, each on a node of its
                %% own. The first `recover' operation starts the servers,
                %% because a process on a simulated node starts only in a
                %% step of the scheduler.
                {module, _} = code:ensure_loaded(redwall_dst_machine),
                {module, _} = code:ensure_loaded(redwall_dst_nosync_io),
                ok = clear_dir(disk_dir()),
                ok = redwall_dst_machine:configure(Cas#{tab => Tab}),
                Ids = redwall_dst_client:coordinator_ids(),
                Nodes = [Node || {_Name, Node} <- Ids],
                [ok = eta_net:place(Node, []) || Node <- Nodes],
                [true = eta_net:with_node(A, fun() -> eta_net:connect_node(B) end) || A <- Nodes, B <- Nodes, A < B],
                true = ets:insert(Tab, [{{node, Node}, true} || Node <- Nodes]),
                {peace, Tab, Ids}
        end,
    ok = eta_net:place(clients, []),
    true = ets:insert(Tab, {{node, clients}, true}),
    %% 1, 2 or 3 StorageServers, each on a node of its own. The first
    %% `recover' operation starts them, because a process on a simulated
    %% node starts only in a step of the scheduler.
    Count = Seed div 2 rem 3 + 1,
    true = ets:insert(Tab, [{{storage_down, Slot}, -1} || Slot <- lists:seq(1, Count)]),
    {ok, #{
        tab => Tab,
        cstate => CState,
        storage_count => Count,
        mode => Mode,
        window => Window,
        faults => maps:get(faults, Config, true),
        watches => Watches,
        watch_clears => WatchClears,
        late_watches => Late,
        seed => Seed,
        %% Whether ClusterControllers recruit the Sequencers of the run.
        controller => maps:get(controller, Config, false),
        %% Whether the workload cuts connections. With `controllers', it
        %% cuts connections between nodes with a ClusterController
        %% process only.
        partitions => maps:get(partitions, Config, true),
        %% The number of the last operation of the run, or `undefined'.
        last_op => maps:get(last_op, Config, undefined),
        %% An ETS table of the caller for the client history of the run,
        %% or `undefined'. See `redwall_dst_history'.
        history => maps:get(history, Config, undefined),
        coordinators_started => false,
        %% Every process that an operation started, oldest first.
        ops => [],
        %% The number of operations that the run has executed.
        n => 0
    }}.

%% The coordinated state, and the operation processes in the order of
%% their start. The scheduler adopts the Sequencers, the
%% Proxies, the LogServers and the StorageServers as children of the
%% processes that start them.
processes(#{cstate := {local, CState}, ops := Ops}) ->
    [CState | [Pid || Pid <- Ops, is_process_alive(Pid)]];
processes(#{ops := Ops}) ->
    [Pid || Pid <- Ops, is_process_alive(Pid)].

%% Starts the StorageServer of the place `Slot'. `N' is the number of the
%% operation, and `Generation' is the number of StorageServers of the
%% place before this one. A new StorageServer has no data. It reads every
%% record from the LogServers of every epoch. The StorageServer runs on
%% the node `storage_Slot_N'. After a kill, an operation process on that
%% node calls this function, and the StorageServer starts on the node of
%% that process.
%%
%% With the planted defect `one_no_clear', the StorageServer of the place
%% 2 does not apply a clear. A run with one StorageServer has no defect.
start_storage(Tab, CState, {Mode, Window}, {Slot, N}, Generation) ->
    ok = redwall_dst_client:join(Tab, Mode),
    {ok, Storage} = redwall_storage:start(#{
        coordinators => redwall_dst_client:coordinators(CState),
        %% The other StorageServers of the run that run. A StorageServer
        %% that starts after a kill copies the data from one of them, if
        %% one of them has data.
        peers => fun() ->
            %% In a run with the pop, a StorageServer of the place 3 that
            %% starts after a kill finds no peer at its first question. It
            %% then reads the log from the start, and a LogServer answers
            %% that it has discarded the first records. A StorageServer
            %% whose peers with data do not answer at its start is in the
            %% same state.
            First = ets:update_counter(Tab, {peer_questions, self()}, 1, {{peer_questions, self()}, 0}) =:= 1,
            case First andalso Slot =:= 3 andalso Generation > 0 andalso ets:member(Tab, pop) of
                true ->
                    bump(starts_from_log_after_pop),
                    [];
                false ->
                    [Peer || Peer <- storages(Tab), Peer =/= self(), is_process_alive(Peer)]
            end
        end,
        poll_ms => 100,
        mvcc_window => Window,
        rollback => Mode =/= no_rollback,
        check_window => Mode =/= no_window_check,
        clear => Mode =/= no_clear andalso not (Mode =:= one_no_clear andalso Slot =:= 2),
        wait => Mode =/= no_wait,
        copy_rows => Mode =/= no_copy_rows,
        watch_trigger => Mode =/= watch_no_trigger,
        %% The time limit of a watch. See "Runs with watches" and "Runs
        %% with late watches".
        watch_timeout_ms =>
            case {ets:lookup(Tab, watches), ets:member(Tab, late_watches)} of
                {[{watches, live}], _} -> infinity;
                {[{watches, true}], true} -> infinity;
                {[{watches, true}], false} -> 300;
                {[], _} -> 30000
            end
    }),
    Node = redwall_dst_client:storage_node({Slot, N}),
    true = ets:insert(Tab, [{{storage_started, {Slot, N}}, Storage}, {{storage, Slot}, Storage, Generation, Node}]),
    %% In a run with the pop, the StorageServer is in the list of the
    %% coordinated state under the name of its place. The StorageServer
    %% of the place 3 is not in the list, so the LogServers can discard a
    %% record that it has not stored.
    _ = ets:member(Tab, pop) andalso Slot =< 2 andalso redwall_dst_client:register_storage(CState, Slot, Storage, 10),
    Storage.

%% The StorageServers that the table of the run has, in the order of
%% their places.
storages(Tab) ->
    [Storage || [_Slot, Storage] <- lists:sort(ets:match(Tab, {{storage, '$1'}, '$2', '_', '_'}))].

%% The first operation of a run is a recovery, which creates epoch 1.
generate(#{n := 0}, Rand) ->
    {recover, Rand};
generate(#{faults := Faults, cstate := CState, watches := Watches}, Rand0) ->
    {Roll, Rand1} = rand:uniform_s(Rand0),
    {R, Rand2} = rand:uniform_s(1000, Rand1),
    Op =
        if
            %% In a run with watches, 1 operation in 4 is a watch. Every
            %% kind of run has a transaction for these values of `Roll'.
            Watches =/= false, Roll >= 0.55, Roll < 0.80 -> {watch, R};
            %% With `faults => storage', the only fault is the kill of a
            %% StorageServer.
            Faults =:= storage, Roll < 0.08 -> recover;
            Faults =:= storage, Roll < 0.14 -> {kill_storage, R};
            Faults =:= storage -> {transaction, R};
            not Faults, Roll < 0.08 -> recover;
            not Faults -> {transaction, R};
            %% Faults are rare. A LogServer that is lost or cut off stops the
            %% commits of its epoch until the next recovery, and only an
            %% acknowledged commit can violate most of the invariants.
            Roll < 0.12 -> recover;
            Roll < 0.93 -> {transaction, R};
            Roll < 0.945 -> {kill_log, R};
            Roll < 0.96 -> {kill_sequencer, R};
            Roll < 0.97 -> {kill_proxy, R};
            Roll < 0.975 -> {kill_resolver, R};
            Roll < 0.98 -> {kill_storage, R};
            Roll < 0.988 -> partition;
            %% Only a run with a peace cluster has this operation, so the
            %% operations of the other runs are the same as before.
            Roll < 0.996, element(1, CState) =:= peace -> {kill_coordinator, R};
            true -> heal
        end,
    case Op of
        partition ->
            {R2, Rand3} = rand:uniform_s(1000, Rand2),
            {{partition, R, R2}, Rand3};
        {transaction, R} ->
            {R2, Rand3} = rand:uniform_s(1000, Rand2),
            {{transaction, R, R2}, Rand3};
        {watch, R} ->
            {R2, Rand3} = rand:uniform_s(1000, Rand2),
            {{watch, R, R2}, Rand3};
        _ ->
            {Op, Rand2}
    end.

execute(Op, S0 = #{n := N0}) ->
    N = N0 + 1,
    S = execute(Op, N, S0#{n := N}),
    case S of
        #{last_op := N, controller := true} -> repair(N, S);
        _ -> S
    end.

%% After the last operation of a run with ClusterControllers, the harness
%% removes every cut, and starts a new worker node for each killed worker
%% node and a new StorageServer for each place without one. From this
%% point the network loses no message, because redwall does not send a
%% message again and a lost message costs the time limit of a call. The
%% system then has the nodes for an epoch, and `check_final/2' checks that
%% it has one. In a run with the config `late_watches', the harness also
%% starts the late watches (`late_watches/2').
repair(N, S00 = #{tab := Tab, cstate := CState, mode := Mode, window := Window}) ->
    ok = eta_net:heal_all(),
    S0 = restart_coordinators(S00),
    ok = eta_net:set_policy((eta_net:policy())#{drop_p => 0.0}),
    S1 = lists:foldl(
        fun([Slot, Generation], Acc) ->
            true = ets:delete(Tab, {storage_down, Slot}),
            spawn_op(redwall_dst_client:storage_node({Slot, N * 100}), fun() -> start_storage(Tab, CState, {Mode, Window}, {Slot, N * 100}, Generation + 1) end, Acc)
        end,
        S0,
        lists:sort(ets:match(Tab, {{storage_down, '$1'}, '$2'}))
    ),
    late_watches(N, replace_workers(N * 100, S1)).

%% Asks the ClusterControllers for a recovery, and starts the clients of
%% the late watches and of the late changes, on the node `clients'. See
%% "Runs with late watches". The late watch `I' has the client number
%% `N * 100 + I', and the late change of the key `I' has the client
%% number `N * 100 + 10 + I'. The late watches with an odd number use the
%% second key of the watches, and the other late watches use the first
%% key. The seed and the number of the late watch select the kind of the
%% transaction.
late_watches(_N, S = #{late_watches := false}) ->
    S;
late_watches(N, S00 = #{tab := Tab, seed := Seed}) ->
    %% A request for a recovery. The epoch of the time of the faults can
    %% have a LogServer without a record that the network lost. Such an
    %% epoch gives read versions and commits no transaction.
    S0 = spawn_op(clients, fun() -> redwall_dst_client:force_recovery(Tab) end, S00),
    S1 = lists:foldl(
        fun(I, Acc) ->
            R = erlang:phash2({Seed, I}, 1000) * 2 + I rem 2,
            spawn_op(clients, fun() -> redwall_dst_client:late_watch(Tab, N * 100 + I, R) end, Acc)
        end,
        S0,
        lists:seq(1, ?LATE_WATCHES)
    ),
    lists:foldl(
        fun(I, Acc) -> spawn_op(clients, fun() -> redwall_dst_client:late_change(Tab, N * 100 + 10 + I, I, ?LATE_WATCHES) end, Acc) end,
        S1,
        lists:seq(1, ?LATE_CHANGES)
    ).

%% Starts a new worker node for each killed worker node, with the class of
%% the killed node, and with a candidate if the killed node had one.
replace_workers(Base, S = #{tab := Tab, cstate := CState, mode := Mode}) ->
    lists:foldl(
        fun({K, [Node, {Class, Candidate}]}, Acc) ->
            true = ets:delete(Tab, {worker_down, Node}),
            spawn_op(redwall_dst_client:worker_node(Base + K), fun() -> redwall_dst_client:worker(Tab, CState, Mode, Class, Candidate) end, Acc)
        end,
        S,
        lists:enumerate(lists:sort(ets:match(Tab, {{worker_down, '$1'}, '$2'})))
    ).

execute(recover, 1, S0 = #{cstate := {peace, Tab, Ids}, coordinators_started := false}) ->
    %% The servers of the peace cluster, and a process that forms the
    %% cluster.
    S1 = lists:foldl(fun(Id, Acc) -> start_coordinator(Id, Acc) end, S0, Ids),
    S2 = spawn_op(clients, fun() -> redwall_dst_client:form_cluster(Tab, Ids) end, S1),
    execute(recover, 1, S2#{coordinators_started := true});
execute({kill_coordinator, R}, _N, S = #{tab := Tab, cstate := {peace, Tab, Ids}}) ->
    %% A kill of the node of a coordinator that runs. The kill is also a
    %% crash of the simulated disk of the node: the node loses the data
    %% that the coordinator has not synced. Any number of coordinators can
    %% be killed, because each one starts again with its data at the next
    %% `recover' operation. The harness kills no coordinator before the
    %% cluster has formed.
    %% 1 operation in 4 kills every coordinator that runs, as a loss of
    %% power of every computer does.
    Running = [Id || Id <- Ids, not ets:member(Tab, {coordinator_down, Id})],
    Targets =
        case R rem 4 of
            0 -> Running;
            _ -> [Id || Id <- [pick(R div 4, Running)], Id =/= none]
        end,
    case ets:member(Tab, cluster_formed) of
        true ->
            lists:foreach(
                fun(Id = {_Name, Node}) ->
                    ok = eta_net:kill_node(Node),
                    true = ets:insert(Tab, {{coordinator_down, Id}, true}),
                    bump(kills_coordinator)
                end,
                Targets
            ),
            _ = Targets =/= [] andalso Targets =:= Running andalso bump(kills_of_every_coordinator),
            S;
        false ->
            S
    end;
execute(recover, N, S00 = #{tab := Tab, cstate := CState, mode := Mode, window := Window, controller := Controller}) ->
    %% The operation starts each killed coordinator again, on its node.
    S0 = restart_coordinators(S00),
    %% The operation also starts a new StorageServer for each place whose
    %% StorageServer was killed without a new one.
    S = lists:foldl(
        fun([Slot, Generation], Acc) ->
            true = ets:delete(Tab, {storage_down, Slot}),
            spawn_op(redwall_dst_client:storage_node({Slot, N}), fun() -> start_storage(Tab, CState, {Mode, Window}, {Slot, N}, Generation + 1) end, Acc)
        end,
        S0,
        lists:sort(ets:match(Tab, {{storage_down, '$1'}, '$2'}))
    ),
    case Controller of
        false ->
            spawn_op(redwall_dst_client:sequencer_node(N), fun() -> redwall_dst_client:recover(Tab, N, CState, Mode) end, S);
        true when N =:= 1 ->
            %% 4 worker nodes of the class `stateless', of which 3 have a
            %% candidate for the ClusterController, and 4 worker nodes of
            %% the class `log'.
            lists:foldl(
                fun(K, Acc) ->
                    Class =
                        case K =< 4 of
                            true -> stateless;
                            false -> log
                        end,
                    spawn_op(redwall_dst_client:worker_node(K), fun() -> redwall_dst_client:worker(Tab, CState, Mode, Class, K =< 3) end, Acc)
                end,
                S,
                lists:seq(1, 8)
            );
        true ->
            %% A new worker node for each killed worker node. Then the
            %% operation asks the ClusterController for a recovery.
            S1 = replace_workers(N * 10, S),
            spawn_op(clients, fun() -> redwall_dst_client:force_recovery(Tab) end, S1)
    end;
execute({transaction, R1, R2}, N, S = #{tab := Tab}) ->
    Proxies = lists:keysort(2, [{Proxy, Epoch} || [Proxy, Epoch] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'}), is_process_alive(Proxy)]),
    %% The Proxy of the highest epoch in 3 of 4 cases, and any Proxy that
    %% runs in the others.
    Choose = fun
        (_R) when Proxies =:= [] -> none;
        (R) when R rem 4 =:= 0 -> pick(R div 4, Proxies);
        (_R) -> lists:last(Proxies)
    end,
    case {Choose(R1), Choose(R2)} of
        {none, none} -> S;
        {Reader, Proxy} ->
            %% 1 client in 2 runs on the node of a StorageServer that is
            %% alive, and reads the ETS table of that StorageServer. The
            %% other clients run on the node `clients', and read by
            %% message.
            Local = [Node || [Storage, Node] <- lists:sort(ets:match(Tab, {{storage, '_'}, '$1', '_', '$2'})), is_process_alive(Storage)],
            Node =
                case {R1 rem 2, Local} of
                    {0, [_ | _]} -> pick(R2, Local);
                    _ -> clients
                end,
            spawn_op(Node, fun() -> redwall_dst_client:transaction(Tab, N, Reader, Proxy, R1 + R2) end, S)
    end;
execute({watch, R1, R2}, N, S = #{tab := Tab}) ->
    %% A client with a watch. 1 client in 2 runs on the node of a
    %% StorageServer that is alive, as the clients of the transactions do.
    Local = [Node || [Storage, Node] <- lists:sort(ets:match(Tab, {{storage, '_'}, '$1', '_', '$2'})), is_process_alive(Storage)],
    Node =
        case {R1 rem 2, Local} of
            {0, [_ | _]} -> pick(R2, Local);
            _ -> clients
        end,
    spawn_op(Node, fun() -> redwall_dst_client:watch(Tab, N, R1 + R2) end, S);
execute({Kill, R}, _N, S = #{tab := Tab, controller := true}) when Kill =:= kill_log; Kill =:= kill_sequencer; Kill =:= kill_proxy; Kill =:= kill_resolver ->
    %% With ClusterControllers, a kill stops the worker node of a role
    %% that runs, with every role on the node. The process of an epoch
    %% does the work of the Sequencer, of the Resolver and of the Proxy,
    %% so the three kills stop the node of such a process.
    {Role, Name} =
        case Kill of
            kill_log -> {log, log};
            kill_sequencer -> {proxy, sequencer};
            kill_proxy -> {proxy, proxy};
            kill_resolver -> {proxy, resolver}
        end,
    Nodes = lists:usort([Node || [Pid, Node] <- ets:match(Tab, {{at, '$1'}, '$2', Role}), is_process_alive(Pid)]),
    case pick(R, Nodes) of
        none -> S;
        Node -> kill_worker(Node, Name, S)
    end;
execute({kill_log, R}, _N, S = #{tab := Tab, cstate := CState}) ->
    case current(CState) of
        #{epoch := Epoch, logs := Logs} ->
            case ets:member(Tab, {killed_log, Epoch}) of
                true ->
                    S;
                false ->
                    {Id, _Pid} = pick(R, Logs),
                    case lists:keymember(Id, 1, log_pids(Tab)) of
                        false ->
                            S;
                        true ->
                            true = ets:insert(Tab, {{killed_log, Epoch}, true}),
                            kill(redwall_dst_client:log_node(Id), kills_log, S)
                    end
            end;
        undefined ->
            S
    end;
execute({kill_sequencer, R}, _N, S = #{tab := Tab}) ->
    case pick(R, [N || [N, Pid] <- ets:match(Tab, {{sequencer, '$1'}, '$2'}), is_process_alive(Pid)]) of
        none -> S;
        N -> kill(redwall_dst_client:sequencer_node(N), kills_sequencer, S)
    end;
execute({kill_proxy, R}, _N, S = #{tab := Tab}) ->
    case pick(R, [N || [N, Pid] <- ets:match(Tab, {{proxy, '$1'}, '$2', '_'}), is_process_alive(Pid)]) of
        none -> S;
        N -> kill(redwall_dst_client:sequencer_node(N), kills_proxy, S)
    end;
execute({kill_resolver, R}, _N, S = #{tab := Tab}) ->
    Running = [N || [N] <- ets:match(Tab, {{recovered, '$1'}, '_', '_', '_'}), not ets:member(Tab, {killed_resolver, N})],
    case pick(R, lists:sort(Running)) of
        none ->
            S;
        N ->
            true = ets:insert(Tab, {{killed_resolver, N}, true}),
            kill(redwall_dst_client:sequencer_node(N), kills_resolver, S)
    end;
execute({kill_storage, R}, N, S = #{tab := Tab, cstate := CState, mode := Mode, window := Window, storage_count := Count}) ->
    Slot = R rem Count + 1,
    case [Row || Row = {_, Storage, _, _} <- ets:lookup(Tab, {storage, Slot}), is_process_alive(Storage), expendable(Tab, Storage)] of
        [{_, Storage, Generation, Node}] ->
            %% The rollbacks of the StorageServer that the harness kills.
            case eta_observe:read(Storage) of
                Observed = #{rollbacks := Rollbacks, copies := Copies} ->
                    bump(rollbacks, Rollbacks),
                    bump(storage_copies, Copies),
                    bump(storage_watches_fired, maps:get(watches_fired, Observed, 0)),
                    bump(storage_watches_timed_out, maps:get(watches_timed_out, Observed, 0)),
                    bump(storage_popped, maps:get(popped, Observed, 0));
                _ ->
                    ok
            end,
            S1 = kill(Node, kills_storage, S),
            case Count > 1 andalso R div Count rem 2 =:= 1 of
                true ->
                    %% In a run with several StorageServers, 1 kill in 2
                    %% leaves the place without a StorageServer until the
                    %% next `recover' operation.
                    true = ets:insert(Tab, {{storage_down, Slot}, Generation}),
                    S1;
                false ->
                    %% An operation process starts the new StorageServer,
                    %% because a process starts only in a step of the
                    %% scheduler.
                    spawn_op(redwall_dst_client:storage_node({Slot, N}), fun() -> start_storage(Tab, CState, {Mode, Window}, {Slot, N}, Generation + 1) end, S1)
            end;
        [] ->
            %% The StorageServer of the place has not started.
            S
    end;
execute({partition, _R1, _R2}, _N, S = #{partitions := false}) ->
    S;
execute({partition, R1, R2}, _N, S = #{tab := Tab, partitions := Partitions}) ->
    Nodes =
        case Partitions of
            %% A cut between two nodes with a ClusterController process
            %% that runs. A candidate then proposes itself while the
            %% leader runs.
            controllers -> lists:sort([Node || [Node, Pid] <- ets:match(Tab, {{controller, '$1'}, '$2'}), is_process_alive(Pid)]);
            _ -> [Node || [Node] <- ets:match(Tab, {{node, '$1'}, '_'})]
        end,
    case {pick(R1, Nodes), pick(R2, Nodes)} of
        {A, A} ->
            S;
        {A, B} ->
            ok = eta_net:partition(A, B),
            bump(partitions),
            S
    end;
execute(heal, _N, S) ->
    ok = eta_net:heal_all(),
    S.

check(#{tab := Tab, cstate := CState}) ->
    %% Trace validation. See `redwall_dst_trace'.
    _ = [redwall_dst_trace:record(Table, Tab, CState) || {trace, Table} <- ets:lookup(Tab, trace)],
    Storages = storages(Tab),
    Acks = [{C, Epoch, LSN} || [C, Epoch, LSN] <- ets:match(Tab, {{ack, '$1'}, '$2', '$3'})],
    Logs = logs(Tab),
    _ = ets:member(Tab, pop) andalso remember_written(Tab, Logs),
    _ = ets:member(Tab, watch_clears) andalso remember_watch_records(Tab, Logs),
    first_violation([
        fun() -> no_loss(Tab) end,
        fun() -> unique_versions(Tab, Acks) end,
        fun() -> durable(Acks, CState) end,
        fun() -> body_available(Acks, Logs) end,
        fun() -> copied(Tab, Acks, CState, Logs) end,
        fun() -> popped_stored(Tab, Logs) end,
        fun() -> transactions(Tab, Acks, Logs) end,
        fun() -> storage_rollback(Tab, Storages, CState) end,
        fun() -> kcv_below_dv(Logs) end,
        fun() -> chain(Logs) end,
        fun() -> one_leader(Tab) end,
        fun() -> select_agrees(Tab) end,
        fun() -> mapped_range_agrees(Tab) end,
        fun() -> range_options_agree(Tab) end,
        fun() -> own_writes(Tab) end,
        fun() -> coordinators_agree(Tab) end,
        fun() -> watches(Tab) end,
        fun() -> watch_rereads(Tab, CState) end
    ]).

%% `watch_change' and `watch_error'. A client with a watch records a
%% violation in the table of the run. See "Runs with watches".
watches(Tab) ->
    case lists:sort(ets:match(Tab, {{watch_violation, '_'}, '$1'})) of
        [] -> ok;
        [[Detail = #{property := Property}] | _] -> violation(Property, Detail)
    end.

%% `watch_change' in a run with the config `watch_clears'. See "Runs with
%% clears of the keys of the watches". A client records the value and the
%% read version of its read after a ready future in the row
%% `{watch_reread, C}'. The harness decides each row once, and then
%% records one of two rows:
%%
%% <ul>
%% <li>`{watch_checked, C}' - the read returned another value than the
%%   value of the watch.</li>
%% <li>`{watch_changed_back, C}' - the read returned the value of the
%%   watch, and the records of the LogServers give the key another value
%%   at a version from the version of the watch to the read version.</li>
%% </ul>
watch_rereads(Tab, CState) ->
    Open = [{C, Reread} || [C, Reread] <- lists:sort(ets:match(Tab, {{watch_reread, '$1'}, '$2'})), not ets:member(Tab, {watch_decided, C})],
    first_violation([fun() -> watch_reread(Tab, CState, C, Reread) end || {C, Reread} <- Open]).

watch_reread(Tab, CState, C, #{value := Seen, read_version := ReadVersion}) ->
    [{_, Watch = #{key := Key, value := Value, version := Version}}] = ets:lookup(Tab, {watch, C}),
    case Seen =/= Value of
        true ->
            true = ets:insert(Tab, [{{watch_decided, C}, true}, {{watch_checked, C}, true}]),
            ok;
        false ->
            History = watch_history(Tab, CState, Key),
            %% The value of the key at the version of the watch, and the
            %% values after the later records up to the read version.
            At = lists:last([not_found | [After || {LSN, After} <- History, LSN =< Version]]),
            Later = [After || {LSN, After} <- History, LSN > Version, LSN =< ReadVersion],
            case [Other || Other <- [At | Later], Other =/= Value] of
                [_ | _] ->
                    true = ets:insert(Tab, [{{watch_decided, C}, true}, {{watch_changed_back, C}, true}]),
                    ok;
                [] ->
                    violation(watch_change, #{client => C, watch => Watch, read_version => ReadVersion, records => [Record || Record = {LSN, _After} <- History, LSN >= Version]})
            end
    end.

%% Keeps the changes of the keys of the watches from the records of the
%% LogServers in the table of the run, in a run with the config
%% `watch_clears'. The row `{watch_record, Key, LSN, Epoch}' has the value
%% of the key after the record `LSN' on a LogServer of `Epoch', or
%% `not_found'. The harness calls this function at each check, because a
%% LogServer can stop, and a LogServer of a run with the pop discards
%% records. For each LogServer the function reads the records above the
%% highest LSN of its last call.
remember_watch_records(Tab, Logs) ->
    Keys = [redwall_dst_client:watch_key(I) || I <- [1, 2]],
    lists:foreach(
        fun({{Epoch, _Index}, #{pid := Pid, records := Records}}) ->
            Seen =
                case ets:lookup(Tab, {watch_records_seen, Pid}) of
                    [{_, LSN}] -> LSN;
                    [] -> -1
                end,
            case gb_trees:is_empty(Records) orelse element(1, gb_trees:largest(Records)) =< Seen of
                true ->
                    true;
                false ->
                    true = ets:insert(Tab, [
                        {{watch_record, Key, LSN, Epoch}, After}
                     || {LSN, {_Prev, {body, {batch, Entries}}}} <- gb_trees:to_list(Records),
                        LSN > Seen,
                        Key <- Keys,
                        {changed, After} <- [value_after(Key, Entries)]
                    ]),
                    true = ets:insert(Tab, {{watch_records_seen, Pid}, element(1, gb_trees:largest(Records))})
            end
        end,
        Logs
    ).

%% The value of `Key' after the mutations of the transactions of one
%% record, in their order: `{changed, Value}', `{changed, not_found}', or
%% `unchanged' for a record without a mutation of the key.
value_after(Key, Entries) ->
    lists:foldl(
        fun
            ({set, Set, Value}, _Acc) when Set == Key -> {changed, Value};
            ({clear, Cleared}, _Acc) when Cleared == Key -> {changed, not_found};
            ({clear_range, Begin, End}, _Acc) when Begin =< Key, Key < End -> {changed, not_found};
            (_Mutation, Acc) -> Acc
        end,
        unchanged,
        lists:append([Entry || Entry <- Entries, is_list(Entry)])
    ).

%% The changes of `Key' that no recovery discarded, in the order of their
%% versions: `{LSN, Value}' with the value of the key after the record
%% `LSN', or `not_found'. A recovery discarded a record in two cases:
%%
%% <ul>
%% <li>The record is on a LogServer of an ended epoch, above the end
%%   version of that epoch.</li>
%% <li>The LSN is between the end version of an ended epoch and the first
%%   version of the next epoch. A LogServer of a recovery that did not
%%   write the coordinated state can have such a record.</li>
%% </ul>
watch_history(Tab, CState, Key) ->
    Old = maps:get(old, cstate(CState), []),
    Ended = maps:from_list([{Epoch, End} || #{epoch := Epoch, end_version := End} <- Old]),
    Ends = [End || End <- maps:values(Ended), End > 0],
    lists:usort([
        {LSN, After}
     || [LSN, Epoch, After] <- ets:match(Tab, {{watch_record, Key, '$1', '$2'}, '$3'}),
        not (is_map_key(Epoch, Ended) andalso LSN > maps:get(Epoch, Ended)),
        not lists:any(fun(End) -> LSN > End andalso LSN < End + ?EPOCH_GAP end, Ends)
    ]).

%% `watch_fires': the future of a watch is ready, if an acknowledged
%% transaction set the key of the watch at a commit version above the
%% version of the watch. The harness checks it at the end of a run with
%% `watches => live' and without the config `watch_clears'.
watch_fires(Tab) ->
    Watches = [{C, Watch} || [C, Watch] <- lists:sort(ets:match(Tab, {{watch, '$1'}, '$2'}))],
    Sets = [{Key, Version} || {_C, #{set := true, key := Key, version := Version}} <- Watches],
    Late = [
        {C, Watch}
     || {C, Watch = #{key := Key, version := Version}} <- Watches,
        not ets:member(Tab, {watch_fired, C}),
        lists:any(fun({K, V}) -> K =:= Key andalso V > Version end, Sets)
    ],
    case Late of
        [] -> ok;
        [{C, Watch} | _] -> violation(watch_fires, #{client => C, watch => Watch, not_ready => length(Late)})
    end.

%% `watch_fires' in a run with the config `watch_clears'. See "Runs with
%% clears of the keys of the watches". `Late' selects the watches: every
%% watch of the run, or the late watches only. The function checks two
%% statements:
%%
%% <ul>
%% <li>Each StorageServer that runs has a version at or above the commit
%%   version of every selected watch whose transaction set or cleared the
%%   key, and of every late change.</li>
%% <li>The future of each selected watch is ready, if a StorageServer
%%   that runs has a version at or above the version of the watch, and the
%%   value of the key at the version of that StorageServer differs from
%%   the value of the watch.</li>
%% </ul>
watch_fires(Tab, Late) ->
    Watches = [{C, Watch} || [C, Watch] <- lists:sort(ets:match(Tab, {{watch, '$1'}, '$2'})), not Late orelse maps:get(late, Watch, false)],
    Changes = [Version || [#{version := Version}] <- ets:match(Tab, {{late_change, '_'}, '$1'})],
    Top = lists:max([0 | Changes] ++ [Version || {_C, #{version := Version, kind := Kind}} <- Watches, Kind =/= read]),
    Running = [{Storage, eta_observe:read(Storage)} || Storage <- storages(Tab), is_process_alive(Storage)],
    Behind = [Storage || {Storage, Published} <- Running, not is_map(Published) orelse maps:get(version, Published, -1) < Top],
    NotReady = [
        {C, Watch, Stored}
     || {C, Watch = #{key := Key, value := Value, version := Version}} <- Watches,
        not ets:member(Tab, {watch_fired, C}),
        {_Storage, Published = #{version := Mine}} <- Running,
        Mine >= Version,
        {ok, Stored} <- [stored(Published, Key)],
        Stored =/= Value
    ],
    case {Behind, NotReady} of
        {[], []} -> ok;
        {[_ | _], _} -> violation(watch_fires, #{reason => storage_behind, version => Top, behind => length(Behind), storages => length(Running)});
        {[], [{C, Watch, Stored} | _]} -> violation(watch_fires, #{client => C, watch => Watch, stored => Stored, not_ready => length(lists:usort([Client || {Client, _, _} <- NotReady]))})
    end.

%% The value of `Key' in a StorageServer at the version of the
%% StorageServer, from its published state: `{ok, Value}',
%% `{ok, not_found}', or `error' if the StorageServer has no table.
stored(#{engine := Engine, view := View, version := Version}, Key) ->
    try Engine:read(View, {key, Key}, Version) of
        {ok, Value, _Written} -> {ok, Value};
        not_found -> {ok, not_found}
    catch
        error:badarg -> error
    end;
stored(_Published, _Key) ->
    error.

%% The checks of the late watches at the end of a run with the config
%% `late_watches'. See "Runs with late watches".
%%
%% <ul>
%% <li>`late_commit' - each client of a late watch and each client of a
%%   late change has a committed transaction.</li>
%% <li>`watch_fires' - the statements of `watch_fires/2' hold for the
%%   late watches.</li>
%% </ul>
late_final(#{late_watches := false}) ->
    ok;
late_final(#{tab := Tab}) ->
    Watches = length([C || [C, #{late := true}] <- ets:match(Tab, {{watch, '$1'}, '$2'})]),
    Changes = length(ets:match(Tab, {{late_change, '_'}, '_'})),
    case {Watches, Changes} of
        {?LATE_WATCHES, ?LATE_CHANGES} -> watch_fires(Tab, true);
        _ -> violation(late_commit, #{watches => Watches, changes => Changes, failed => lists:sort(ets:match(Tab, {{late_failed, '$1'}, '$2'}))})
    end.

%% A `select/4' of a client returned the result of the match spec on the
%% result of `get_range/3' at the same read version.
select_agrees(Tab) ->
    case ets:match(Tab, {{select_differs, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(select_agrees, Detail)
    end.

%% No two servers of the peace cluster have two states of the coordinated
%% state for one index of the log of peace. See `redwall_dst_machine'.
coordinators_agree(Tab) ->
    case ets:match(Tab, {{cstate_differs, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(coordinators_agree, Detail)
    end.

%% The reads of a transaction returned its own sets and clears, and
%% `on_error/2' left a transaction without a read version and without a
%% mutation.
own_writes(Tab) ->
    case ets:match(Tab, {{own_writes_differ, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(own_writes, Detail)
    end.

%% A `get_range/4' of a client with the options `reverse' and `limit',
%% and a `get_key/2', returned the results that follow from `get_range/3'
%% at the same read version.
range_options_agree(Tab) ->
    case ets:match(Tab, {{range_options_differ, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(range_options_agree, Detail)
    end.

%% A `get_mapped_range/4' of a client returned the rows of `get_range/3'
%% at the same read version, each with the result of its second read.
mapped_range_agrees(Tab) ->
    case ets:match(Tab, {{mapped_differs, '_'}, '$1'}) of
        [] -> ok;
        [[Detail] | _] -> violation(mapped_range_agrees, Detail)
    end.

%% The ClusterController processes that run and are the leader, each with
%% its term and its information about the database.
leaders(Tab) ->
    [
        {Term, Pid, Info}
     || [Pid] <- lists:sort(ets:match(Tab, {{controller, '_'}, '$1'})),
        is_process_alive(Pid),
        #{role := leader, term := Term, db_info := Info} <- [eta_observe:read(Pid)]
    ].

%% No two ClusterController processes are the leader of one term. Two
%% leaders of two terms can run at the same time. The harness counts the
%% runs with that state.
one_leader(Tab) ->
    Terms = [Term || {Term, _Pid, _Info} <- leaders(Tab)],
    _ = length(Terms) > 1 andalso ets:insert(Tab, {two_leaders, true}),
    case {Terms -- lists:usort(Terms), ets:match(Tab, {{second_leader, '$1'}, '_'})} of
        {[], []} -> ok;
        {[Term | _], _} -> violation(one_leader, #{term => Term});
        {_, [[Term] | _]} -> violation(one_leader, #{term => Term})
    end.

%% The checks at the end of a run with ClusterControllers
%% (`controller_final/1'). Before the end, the harness has removed every
%% cut and has replaced every killed worker node (`repair/2'). A run with
%% the config `late_watches' also has the checks of `late_final/1'.
%%
%% <ul>
%% <li>`leader' - the ClusterController of the coordinated state runs and
%%   is the leader of the term of the coordinated state.</li>
%% <li>`stale_leader' - no other process is a leader.</li>
%% <li>`epoch' - the leader has an epoch, which is the current epoch of
%%   the coordinated state, and the process of the epoch runs.</li>
%% <li>`worker_db_info' - each worker that runs has the information of
%%   the leader about the database.</li>
%% </ul>
check_final(_Settled, State) ->
    %% The statistics of the run are recorded here, while the scheduler
    %% still holds every process. See `record_run_stats/1'.
    ok = record_run_stats(State),
    final_checks(State).

final_checks(State = #{controller := true, last_op := Last}) when is_integer(Last) ->
    first_violation([fun() -> controller_final(State) end, fun() -> late_final(State) end]);
final_checks(#{tab := Tab, watches := live, watch_clears := true}) ->
    watch_fires(Tab, false);
final_checks(#{tab := Tab, watches := live}) ->
    watch_fires(Tab);
final_checks(_State) ->
    ok.

controller_final(#{tab := Tab, cstate := CState}) ->
    {Term, Controller} = maps:get(controller, cstate(CState), {0, undefined}),
    Leaders = leaders(Tab),
    Workers = [
        {Node, eta_observe:read(Pid)}
     || [Node, Pid] <- lists:sort(ets:match(Tab, {{worker, '$1'}, '$2', '_'})), is_process_alive(Pid)
    ],
    case lists:keyfind(Controller, 2, Leaders) of
        {Term, Controller, Info} ->
            Epoch =
                case current(CState) of
                    #{epoch := E} -> E;
                    undefined -> 0
                end,
            Alive = fun(Pid) -> is_pid(Pid) andalso is_process_alive(Pid) end,
            first_violation([
                fun() ->
                    case [T || {T, Pid, _} <- Leaders, Pid =/= Controller] of
                        [] -> ok;
                        Stale -> violation(stale_leader, #{terms => Stale, term => Term})
                    end
                end,
                fun() ->
                    case Info of
                        #{epoch := Epoch, proxies := [Proxy]} ->
                            case Alive(Proxy) of
                                true -> ok;
                                false -> violation(epoch, #{epoch => Epoch, reason => role_down})
                            end;
                        #{epoch := Other} ->
                            violation(epoch, #{epoch => Other, current => Epoch})
                    end
                end,
                fun() ->
                    case [Node || {Node, Observed} <- Workers, not is_map(Observed) orelse maps:get(db_info, Observed, undefined) =/= Info] of
                        [] -> ok;
                        Nodes -> violation(worker_db_info, #{nodes => Nodes, workers => length(Workers)})
                    end
                end
            ]);
        _ ->
            violation(leader, #{term => Term, leaders => [T || {T, _, _} <- Leaders]})
    end.

%% Ends a run with ClusterControllers when the checks of `check_final/2'
%% hold. Such a run has a large number of settle steps, because a call
%% that was in progress at the last operation can wait for its time
%% limit.
settle_done(State = #{controller := true, last_op := Last}) when is_integer(Last) ->
    final_checks(State) =:= ok;
settle_done(State = #{watches := live}) ->
    final_checks(State) =:= ok;
settle_done(_State) ->
    false.

%% Records the statistics of the run, once for each run.
%%
%% eta calls `check_final/2' at the end of a run that has no violation,
%% before it releases the processes of the run. eta calls `terminate/1'
%% after the release. A released process runs in real time: a client can
%% write one more row to the table of the run, and a StorageServer can
%% apply one more record, before `terminate/1' reads them. The sums of two
%% runs of one seed then differed by 1 in a counter. `check_final/2'
%% therefore records the statistics. `terminate/1' records them only for a
%% run that ended without a call of `check_final/2'.
record_run_stats(#{tab := Tab, cstate := CState}) ->
    case ets:insert_new(Tab, {stats_recorded, true}) of
        true -> record_run_stats(Tab, CState);
        false -> ok
    end.

record_run_stats(Tab, CState) ->
    ok = record_stats(Tab),
    _ = ets:member(Tab, watch_clears) andalso record_watch_stats(Tab, CState),
    lists:foreach(
        fun(Storage) ->
            case eta_observe:read(Storage) of
                Observed = #{rollbacks := Rollbacks, copies := Copies} ->
                    bump(rollbacks, Rollbacks),
                    bump(storage_copies, Copies),
                    %% The answers of the StorageServers to watches.
                    bump(storage_watches_fired, maps:get(watches_fired, Observed, 0)),
                    bump(storage_watches_timed_out, maps:get(watches_timed_out, Observed, 0)),
                    %% The answers `popped' of a LogServer with a popped
                    %% version above the version of the StorageServer.
                    bump(storage_popped, maps:get(popped, Observed, 0));
                _ ->
                    ok
            end
        end,
        storages(Tab)
    ),
    ok.

terminate(State = #{tab := Tab, cstate := CState, ops := Ops}) ->
    ok = record_run_stats(State),
    ok = redwall_dst_history:copy(Tab, maps:get(history, State)),
    %% A ClusterController, a worker and a StorageServer start temporary
    %% processes with a monitor and without a link: a read of the
    %% coordinated state, a pop, a recruitment, a question to the peers.
    %% Such a process does not end with the process that started it. The
    %% harness kills it too. Otherwise it runs after the end of the run,
    %% in real time, and can act during the next run of the same Erlang
    %% node.
    Parents =
        [Pid || [Pid] <- ets:match(Tab, {{controller, '_'}, '$1'})] ++
            [Pid || [Pid] <- ets:match(Tab, {{worker, '_'}, '$1', '_'})] ++
            [Pid || [Pid] <- ets:match(Tab, {{storage_started, '_'}, '$1'})],
    Temporary = lists:usort([
        Child
     || Parent <- Parents, {monitors, Monitors} <- [erlang:process_info(Parent, monitors)], {process, Child} <- Monitors, is_pid(Child)
    ]),
    %% The ClusterControllers and the workers are killed first, so that
    %% no ClusterController recruits after this point.
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{controller, '_'}, '$1'})]),
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{worker, '_'}, '$1', '_'})]),
    ok = kill_all(Temporary),
    Started =
        [Pid || [Pid] <- ets:match(Tab, {{sequencer, '_'}, '$1'})] ++
            [Pid || [Pid] <- ets:match(Tab, {{resolver, '_'}, '$1'})] ++
            [Pid || [Pid, _] <- ets:match(Tab, {{proxy, '_'}, '$1', '$2'})],
    %% The Sequencers are killed first, so that no recovery recruits a
    %% LogServer after the harness has read the table of the run.
    %% The process of a watch is not linked to its client, and it does not
    %% end with its client while it waits for the answer to a read.
    Watchers = [Pid || [Pid] <- ets:match(Tab, {{watcher, '_', '_'}, '$1'})],
    ok = kill_all(Started ++ Ops ++ Watchers),
    ok = kill_all([Pid || {_Id, Pid} <- log_pids(Tab)]),
    ok = kill_all([Pid || [Pid] <- ets:match(Tab, {{storage_started, '_'}, '$1'})] ++ [Pid || {local, Pid} <- [CState]]),
    ok = redwall_dst_machine:clear(),
    _ = [clear_dir(disk_dir()) || {peace, _Tab, _Ids} <- [CState]],
    catch ets:delete(Tab),
    ok.

%% ---------------------------------------------------------------------------
%% Statistics
%% ---------------------------------------------------------------------------

%% The sums over the runs since `reset_stats/0'. A test uses the sums to
%% show that the runs reached the states that the test is about.
stats() ->
    case ets:whereis(?STATS) of
        undefined -> #{};
        _ -> maps:from_list(ets:tab2list(?STATS))
    end.

reset_stats() ->
    case ets:whereis(?STATS) of
        undefined ->
            Owner = spawn(fun() ->
                receive
                    stop -> ok
                end
            end),
            ?STATS = ets:new(?STATS, [named_table, public, {heir, Owner, none}]),
            ok;
        _ ->
            true = ets:delete_all_objects(?STATS),
            ok
    end.

bump(Key) ->
    bump(Key, 1).

bump(Key, N) ->
    case ets:whereis(?STATS) of
        undefined -> ok;
        _ -> _ = ets:update_counter(?STATS, Key, N, {Key, 0})
    end,
    ok.

record_stats(Tab) ->
    Epochs = [Epoch || [Epoch] <- ets:match(Tab, {{recovered, '_'}, '$1', '_', '_'})],
    bump(runs),
    bump(selects, case ets:lookup(Tab, selects) of [{selects, Selects}] -> Selects; [] -> 0 end),
    bump(mapped_reads, case ets:lookup(Tab, mapped_reads) of [{mapped_reads, Mapped}] -> Mapped; [] -> 0 end),
    %% The counters of a run with mixed keys: the reads and the
    %% acknowledged writes of the number key in each of its two forms,
    %% and the rows of a range read with a key in another form than the
    %% form of the client. The other runs do not have these statistics.
    [
        bump(Counter, case ets:lookup(Tab, Counter) of [{Counter, Count}] -> Count; [] -> 0 end)
     || ets:member(Tab, keys),
        Counter <- [integer_key_reads, float_key_reads, integer_key_writes, float_key_writes, other_form_rows]
    ],
    bump(range_option_reads, case ets:lookup(Tab, range_option_reads) of [{range_option_reads, Optioned}] -> Optioned; [] -> 0 end),
    [
        bump(Counter, case ets:lookup(Tab, Counter) of [{Counter, Count}] -> Count; [] -> 0 end)
     || Counter <- [own_mapped_reads, own_write_checks, on_errors]
    ],
    %% The watches: the committed ones, the ready futures, the reads
    %% after a ready future, the futures with the error of a failed
    %% commit, the futures that were not ready in the time of the client,
    %% and the clients whose first read failed.
    [
        bump(Stat, length(ets:match(Tab, {{Row, '_'}, '_'})))
     || {Stat, Row} <- [
            {watches, watch},
            {watches_ready, watch_fired},
            {watch_rereads, watch_checked},
            {watch_commit_errors, watch_error},
            {watches_not_ready, watch_pending},
            {watch_no_read, watch_no_read}
        ]
    ],
    bump(runs_with_two_leaders, length(ets:lookup(Tab, two_leaders))),
    bump(leaders, length(ets:match(Tab, {{leader, '_'}, '_'}))),
    bump(role_failures, length(ets:match(Tab, {{role_failed, '_'}, '_'}))),
    bump(superseded, length(ets:match(Tab, {{superseded, '_'}, '_'}))),
    bump(acks, length(ets:match(Tab, {{ack, '_'}, '_', '_'}))),
    bump(unknown, length(ets:match(Tab, {{unknown, '_'}, '_'}))),
    bump(not_committed, length(ets:match(Tab, {{rejected, '_'}, not_committed}))),
    bump(too_old, length(ets:match(Tab, {{rejected, '_'}, transaction_too_old}))),
    bump(no_read_version, length(ets:match(Tab, {{no_read_version, '_'}, '_'}))),
    bump(no_read, length(ets:match(Tab, {{no_read, '_'}, '_'}))),
    %% The reads below the MVCC window of the StorageServer.
    bump(read_too_old, length(ets:match(Tab, {{read_too_old, '_'}, '_'}))),
    %% The acknowledged transactions that have their LSN together with
    %% another acknowledged transaction.
    LSNs = [LSN || [LSN] <- ets:match(Tab, {{ack, '_'}, '_', '$1'})],
    Repeated = LSNs -- lists:usort(LSNs),
    bump(batched, length([LSN || LSN <- LSNs, lists:member(LSN, Repeated)])),
    bump(reads, length([x || [#{values := [_ | _]}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    %% The transactions with reads from a StorageServer that the harness
    %% started after a kill.
    bump(reads_after_restart, length([x || [#{values := [_ | _], storage := G}] <- ets:match(Tab, {{read, '_'}, '$1'}), G > 0])),
    %% The transactions with reads at a time when a StorageServer of the
    %% run was not alive, in a run with several StorageServers.
    bump(reads_with_one_down, length([x || [#{values := [_ | _], down := {Down, Count}}] <- ets:match(Tab, {{read, '_'}, '$1'}), Down > 0, Count > 1])),
    bump(reads_by_message, length([x || [#{values := [_ | _], direct := false}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    %% The transactions with reads of a client on the node of a
    %% StorageServer, with direct reads.
    bump(reads_on_storage_node, length([x || [#{values := [_ | _], direct := true, local := true}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    bump(storages, length(ets:match(Tab, {{storage, '_'}, '_', '_', '_'}))),
    %% The LogServers that run at the end of the run and have discarded
    %% records after a pop.
    bump(popped_logs, length([x || {_Id, #{popped := Popped}} <- logs(Tab), Popped > 0])),
    %% The acknowledged transactions that cleared a key.
    bump(clears, length([x || [#{clears := [_ | _]}] <- ets:match(Tab, {{txn, '_'}, '$1'})])),
    bump(range_reads, length([x || [#{ranges := [_ | _]}] <- ets:match(Tab, {{read, '_'}, '$1'})])),
    %% The acknowledged transactions with a read range.
    bump(range_acks, length([x || [#{ranges := [_ | _]}] <- ets:match(Tab, {{txn, '_'}, '$1'})])),
    bump(recoveries, length(Epochs)),
    bump(recoveries_failed, length(ets:match(Tab, {{recovery_failed, '_'}, '_'}))),
    %% The number of failed recoveries for each kind of reason.
    lists:foreach(
        fun
            ([{exit, Reason}]) when is_tuple(Reason) -> bump({recovery_failed, exit, element(1, Reason)});
            ([{exit, Reason}]) -> bump({recovery_failed, exit, Reason});
            ([Reason]) when is_tuple(Reason) -> bump({recovery_failed, element(1, Reason)});
            ([Reason]) -> bump({recovery_failed, Reason})
        end,
        ets:match(Tab, {{recovery_failed, '_'}, '$1'})
    ),
    bump(runs_with_three_epochs, length([x || lists:max([0 | Epochs]) >= 3])),
    ok.

%% The statistics of a run with the config `watch_clears':
%%
%% <ul>
%% <li>`watches_set', `watches_read' and `watches_clear' - the committed
%%   watches of each kind of transaction.</li>
%% <li>`watches_ready_set', `watches_ready_read' and
%%   `watches_ready_clear' - the ready futures of each kind.</li>
%% <li>`watches_changed_back' - the reads after a ready future that
%%   returned the value of the watch, after a change and a change back of
%%   the key.</li>
%% <li>`watch_keys_changed_back' - the records that no recovery
%%   discarded and that gave a key of the watches the value that the key
%%   had before the previous change of the key.</li>
%% <li>`watches_waiting_after_change' - the watches whose future is not
%%   ready at the end of the run, and whose key had another value than
%%   the value of the watch after a record above the version of the
%%   watch. The clients of a run with faults stop to wait after 3,000
%%   ms.</li>
%% <li>`late_watches', `late_watches_ready' and `late_changes' - the
%%   committed late watches, their ready futures, and the committed late
%%   changes.</li>
%% <li>`late_changes_above' - the late changes with a commit version
%%   above the version of every late watch of their key.</li>
%% <li>`late_retries' and `late_recoveries' - the failed attempts of the
%%   late clients, and their requests for a recovery.</li>
%% </ul>
record_watch_stats(Tab, CState) ->
    bump(watches_changed_back, length(ets:match(Tab, {{watch_changed_back, '_'}, '_'}))),
    Histories = #{Key => watch_history(Tab, CState, Key) || Key <- [redwall_dst_client:watch_key(I) || I <- [1, 2]]},
    Back = fun
        Back([{_, Value}, Middle = {_, Other} | Rest = [{_, Value} | _]]) when Other =/= Value -> 1 + Back([Middle | Rest]);
        Back([_ | Rest]) -> Back(Rest);
        Back([]) -> 0
    end,
    bump(watch_keys_changed_back, lists:sum([Back([{0, not_found} | History]) || History <- maps:values(Histories)])),
    bump(
        watches_waiting_after_change,
        length([
            C
         || [C, #{key := Key, value := Value, version := Version}] <- ets:match(Tab, {{watch, '$1'}, '$2'}),
            not ets:member(Tab, {watch_fired, C}),
            lists:any(fun({LSN, After}) -> LSN > Version andalso After =/= Value end, maps:get(Key, Histories))
        ])
    ),
    bump(late_changes, length(ets:match(Tab, {{late_change, '_'}, '_'}))),
    Watches = [{C, Watch, ets:member(Tab, {watch_fired, C})} || [C, Watch] <- ets:match(Tab, {{watch, '$1'}, '$2'})],
    lists:foreach(
        fun({_C, #{kind := Kind}, Ready}) ->
            bump(list_to_atom(lists:concat(["watches_", Kind]))),
            _ = Ready andalso bump(list_to_atom(lists:concat(["watches_ready_", Kind])))
        end,
        Watches
    ),
    Late = [{Watch, Ready} || {_C, Watch = #{late := true}, Ready} <- Watches],
    bump(late_watches, length(Late)),
    bump(late_watches_ready, length([x || {_Watch, true} <- Late])),
    %% The late changes with a commit version above the version of every
    %% late watch of their key.
    bump(
        late_changes_above,
        length([
            x
         || [#{key := Key, version := Version}] <- ets:match(Tab, {{late_change, '_'}, '$1'}),
            Late =/= [],
            lists:all(fun({#{key := Watched, version := Watch}, _Ready}) -> Watched =/= Key orelse Watch < Version end, Late)
        ])
    ),
    [
        bump(Counter, case ets:lookup(Tab, Counter) of [{Counter, Count}] -> Count; [] -> 0 end)
     || ets:member(Tab, late_watches), Counter <- [late_retries, late_recoveries]
    ],
    ok.

%% ---------------------------------------------------------------------------
%% Invariants
%% ---------------------------------------------------------------------------

no_loss(Tab) ->
    case [Reason || [Reason = {missing_body, _}] <- ets:match(Tab, {{recovery_failed, '_'}, '$1'})] of
        [] -> ok;
        [Reason | _] -> violation(no_loss, #{reason => Reason})
    end.

unique_versions(Tab, Acks) ->
    ByLSN = lists:foldl(
        fun({C, Epoch, LSN}, Acc) ->
            [{_, #{index := Index}}] = ets:lookup(Tab, {txn, C}),
            maps:update_with(LSN, fun(L) -> [{Epoch, Index, C} | L] end, [{Epoch, Index, C}], Acc)
        end,
        #{},
        Acks
    ),
    %% The acknowledgements of one LSN have one epoch, and each has its own
    %% index.
    Same = fun(Who) ->
        length(lists:usort([Epoch || {Epoch, _Index, _C} <- Who])) > 1 orelse
            length(lists:usort([Index || {_Epoch, Index, _C} <- Who])) < length(Who)
    end,
    case [{LSN, Who} || {LSN, Who = [_, _ | _]} <- maps:to_list(ByLSN), Same(Who)] of
        [] -> ok;
        [{LSN, Who} | _] -> violation(unique_versions, #{lsn => LSN, acknowledged => lists:sort(Who)})
    end.

durable(Acks, CState) ->
    Ended = maps:from_list([{Epoch, End} || #{epoch := Epoch, end_version := End} <- maps:get(old, cstate(CState), [])]),
    case [{C, Epoch, LSN, maps:get(Epoch, Ended)} || {C, Epoch, LSN} <- Acks, is_map_key(Epoch, Ended), LSN > maps:get(Epoch, Ended)] of
        [] ->
            ok;
        [{C, Epoch, LSN, End} | _] ->
            violation(durable, #{client => C, epoch => Epoch, lsn => LSN, end_version => End})
    end.

body_available(Acks, Logs) ->
    Missing = [
        {C, Epoch, LSN}
     || {C, Epoch, LSN} <- Acks,
        not lists:any(
            fun
                ({{E, _Index}, Log = #{records := Records}}) when E =:= Epoch ->
                    LSN =< maps:get(popped, Log, 0) orelse has_body(LSN, C, Records);
                (_) ->
                    false
            end,
            Logs
        )
    ],
    case Missing of
        [] -> ok;
        [{C, Epoch, LSN} | _] -> violation(body_available, #{client => C, epoch => Epoch, lsn => LSN})
    end.

%% `Successors' maps an ended epoch to the epoch that ended it, from the
%% order of the log systems in the coordinated state. The table of the run
%% has the PEV and the RV of each recovery that ended.
copied(Tab, Acks, CState, Logs) ->
    #{current := Current, old := Old} = maps:merge(#{current => undefined, old => []}, cstate(CState)),
    Chain = [Epoch || #{epoch := Epoch} <- [Current | Old], Current =/= undefined],
    Successors = maps:from_list(lists:zip(tl(Chain ++ [none]), Chain)),
    Ranges = maps:from_list([{Epoch, {PEV, RV}} || [Epoch, PEV, RV] <- ets:match(Tab, {{recovered, '_'}, '$1', '$2', '$3'})]),
    Missing = [
        {C, Epoch, LSN, Id}
     || {C, Epoch, LSN} <- Acks,
        is_map_key(Epoch, Successors),
        New <- [maps:get(Epoch, Successors)],
        is_map_key(New, Ranges),
        {PEV, RV} <- [maps:get(New, Ranges)],
        LSN > PEV,
        LSN =< RV,
        {Id = {E, _Index}, Log = #{records := Records}} <- Logs,
        E =:= New,
        LSN > maps:get(popped, Log, 0),
        not has_body(LSN, C, Records)
    ],
    case Missing of
        [] -> ok;
        [{C, Epoch, LSN, Id} | _] -> violation(copied, #{client => C, epoch => Epoch, lsn => LSN, log => Id})
    end.

%% A StorageServer that runs has a version at or above the popped version
%% of every LogServer that runs. A LogServer discards a record that every
%% StorageServer of the cluster has stored, and a StorageServer keeps its
%% data in memory. The harness keeps one StorageServer with the discarded
%% records (`expendable/2').
popped_stored(Tab, Logs) ->
    case lists:max([0 | [maps:get(popped, Log, 0) || {_Id, Log} <- Logs]]) of
        0 ->
            ok;
        Popped ->
            Versions = [storage_version(Storage) || Storage <- storages(Tab), is_process_alive(Storage)],
            case lists:max([0 | Versions]) >= Popped of
                true -> ok;
                false -> violation(popped_stored, #{popped => Popped, versions => Versions})
            end
    end.

%% The version of a StorageServer from its published state, or 0.
storage_version(Storage) ->
    case eta_observe:read(Storage) of
        #{version := Version} -> Version;
        _ -> 0
    end.

%% Whether the harness can kill the StorageServer. In a run with the pop,
%% another StorageServer must run with a version above 0 that is at or
%% above the version of this one. A pop version is at or below the
%% version of a StorageServer that answered for it, so a StorageServer
%% with every discarded record then runs after the kill.
expendable(Tab, Storage) ->
    not ets:member(Tab, pop) orelse
        begin
            Mine = storage_version(Storage),
            lists:any(
                fun(Other) ->
                    Other =/= Storage andalso is_process_alive(Other) andalso storage_version(Other) > 0 andalso storage_version(Other) >= Mine
                end,
                storages(Tab)
            )
        end.

%% The record `LSN' has a body with an entry of the client `C'.
has_body(LSN, C, Records) ->
    case gb_trees:lookup(LSN, Records) of
        {value, {_Prev, {body, {batch, Entries}}}} -> 
            lists:any(
                fun
                    (Entry) when is_list(Entry) -> lists:any(fun(Mutation) -> match_client(Mutation, C) end, Entry);
                    (_Other) -> false
                end,
                Entries
            );
        _ -> false
    end.

match_client({set, _Key, {client, Writer}}, C) -> Writer =:= C;
match_client(_Mutation, _C) -> false.

%% `no_conflict' and `read_version_order'. `no_conflict' compares every
%% pair of acknowledged transactions. `read_version_order' compares every
%% read version with every acknowledged transaction. The harness checks
%% them only when the number of acknowledged transactions or the number of
%% read versions has changed.
transactions(Tab, Acks, Logs) ->
    Reads = [Read#{client => C} || [C, Read] <- ets:match(Tab, {{read, '$1'}, '$2'})],
    Rejected = [C || [C] <- ets:match(Tab, {{rejected, '$1'}, '_'})],
    Count = {length(Acks), length(Reads), length([x || #{values := _} <- Reads]), length(Rejected)},
    case ets:lookup(Tab, transactions_checked) of
        [{_, Count}] ->
            ok;
        _ ->
            Txns = [
                Txn#{client => C, epoch => Epoch, version => LSN}
             || {C, Epoch, LSN} <- Acks, [{_, Txn}] <- [ets:lookup(Tab, {txn, C})]
            ],
            case first_violation([fun() -> no_conflict(Txns) end, fun() -> read_version_order(Reads, Txns) end, fun() -> read_value(Reads, Txns, open_clears(Tab, Acks, Rejected), written(Tab, Logs)) end]) of
                ok ->
                    true = ets:insert(Tab, {transactions_checked, Count}),
                    ok;
                Violation ->
                    Violation
            end
    end.

no_conflict(Txns) ->
    Conflicts = [
        {T, W}
     || T = #{read_version := ReadVersion, version := Version, index := Index, reads := Reads} <- Txns,
        W = #{version := Written, index := WriterIndex, writes := Writes, clears := Clears} <- Txns,
        Written > ReadVersion,
        %% An earlier version, or an earlier transaction of the batch.
        {Written, WriterIndex} < {Version, Index},
        %% A key that the reader read and the writer wrote or cleared.
        [Key || Key <- Writes ++ Clears, lists:member(Key, Reads)] =/= []
    ],
    case Conflicts of
        [] ->
            ok;
        [{T, W} | _] ->
            violation(no_conflict, #{
                reader => maps:with([client, epoch, read_version, version, index, reads], T),
                writer => maps:with([client, epoch, version, index, writes, clears], W)
            })
    end.

%% `Reads' are the read versions that the clients received, also of the
%% transactions that did not commit.
read_version_order(Reads, Txns) ->
    Stale = [
        {T, A}
     || T = #{read_version := ReadVersion, start := Start} <- Reads,
        A = #{version := Version, 'end' := End} <- Txns,
        End < Start,
        ReadVersion < Version
    ],
    case Stale of
        [] ->
            ok;
        [{T, A} | _] ->
            violation(read_version_order, #{
                reader => maps:with([client, reader_epoch, read_version], T),
                acknowledged => maps:with([client, epoch, version], A)
            })
    end.

%% The keys that a transaction without a known result cleared: the cleared
%% keys of every client that started a commit and received neither an
%% acknowledgement nor a rejection. Such a clear can be applied.
open_clears(Tab, Acks, Rejected) ->
    Done = [C || {C, _Epoch, _LSN} <- Acks] ++ Rejected,
    lists:usort(lists:append([Clears || [C, Clears] <- ets:match(Tab, {{attempt, '$1'}, '$2'}), not lists:member(C, Done)])).

%% The LSN of the record of each client, from the records of the
%% LogServers that run. A client commits one transaction, and every write
%% of the transaction has the value `{client, C}'.
written(Tab, Logs) ->
    case ets:member(Tab, pop) of
        true -> maps:from_list([{C, LSN} || [C, LSN] <- ets:match(Tab, {{written, '$1'}, '$2'})]);
        false -> written(Logs)
    end.

%% Keeps the LSN of the record of each client in the table of the run. In
%% a run with the pop, a LogServer discards records, and the harness
%% needs the LSN of a discarded record for `read_value'. The harness
%% calls this function at each check, so it reads each record before a
%% LogServer discards it. For each LogServer it reads the records above
%% the Durable Version of its last call.
remember_written(Tab, Logs) ->
    lists:foreach(
        fun({Id, #{dv := DV, records := Records}}) ->
            Seen =
                case ets:lookup(Tab, {written_dv, Id}) of
                    [{_, Version}] -> Version;
                    [] -> -1
                end,
            case DV > Seen of
                true ->
                    true = ets:insert(Tab, [
                        {{written, C}, LSN}
                     || {LSN, {_Prev, {body, {batch, Entries}}}} <- gb_trees:to_list(Records),
                        LSN > Seen,
                        Entry <- Entries,
                        is_list(Entry),
                        {set, _Key, {client, C}} <- Entry
                    ]),
                    true = ets:insert(Tab, {{written_dv, Id}, DV});
                false ->
                    true
            end
        end,
        Logs
    ).

written(Logs) ->
    maps:from_list([
        {C, LSN}
     || {_Id, #{records := Records}} <- Logs,
        {LSN, {_Prev, {body, {batch, Entries}}}} <- gb_trees:to_list(Records),
        Entry <- Entries,
        is_list(Entry),
        {set, _Key, {client, C}} <- Entry
    ]).

%% `Reads' have the values that the clients read through the client
%% library: `{Key, not_found}' or `{Key, {Value, unknown}}'. The value of a
%% write is `{client, C}'. The LSN of the record that wrote the value is
%% the commit version of the client `C' if `C' received an
%% acknowledgement, and else the LSN of `Written'. `OpenClears' are the
%% keys of `open_clears/3'.
read_value(Reads, Txns, OpenClears, Written) ->
    ByClient0 = maps:from_list([{C, T} || T = #{client := C} <- Txns]),
    ByClient = {ByClient0, Written},
    Wrong = [
        {R, Key, Found, Reason}
     || R = #{read_version := ReadVersion, values := Values} <- Reads,
        {Key, Found} <- Values,
        Reason <- wrong_value(Key, Found, ReadVersion, ByClient, Txns, OpenClears)
    ],
    case Wrong of
        [] ->
            ok;
        [{R, Key, Found, Reason} | _] ->
            violation(read_value, #{
                reader => maps:with([client, reader_epoch, read_version], R),
                key => Key,
                found => Found,
                reason => Reason
            })
    end.

%% The reasons why the value `Found' is wrong. The empty list means that
%% the value is correct.
wrong_value(Key, not_found, ReadVersion, _ByClient, Txns, OpenClears) ->
    %% The key has no value if the last acknowledged transaction that
    %% changed the key cleared it, or if a transaction without a known
    %% result cleared the key.
    case lists:sort([{{Version, Index}, T} || T = #{version := Version, index := Index} <- changers(Key, 0, ReadVersion, Txns)]) of
        [] ->
            [];
        Changers ->
            {_Order, Last = #{clears := Clears}} = lists:last(Changers),
            [{missed, maps:with([client, epoch, version], Last)} || not lists:member(Key, Clears), not lists:member(Key, OpenClears)]
    end;
wrong_value(Key, {{client, Writer}, unknown}, ReadVersion, {ByClient, Logged}, Txns, OpenClears) ->
    case {ByClient, Logged} of
        {#{Writer := #{version := Written}}, _} ->
            wrong_value(Key, {{client, Writer}, Written}, ReadVersion, ByClient, Txns, OpenClears);
        {_, #{Writer := Written}} ->
            wrong_value(Key, {{client, Writer}, Written}, ReadVersion, ByClient, Txns, OpenClears);
        _NoRecordOnALogServerThatRuns ->
            []
    end;
wrong_value(Key, {{client, Writer}, Written}, ReadVersion, ByClient, Txns, _OpenClears) ->
    [above_read_version || Written > ReadVersion] ++
        case ByClient of
            #{Writer := #{version := Version, writes := Writes}} ->
                [{writer_committed_at, Version} || Version =/= Written] ++ [writer_did_not_write_key || not lists:member(Key, Writes)];
            _ ->
                []
        end ++
        [{missed, maps:with([client, epoch, version], T)} || T <- changers(Key, Written, ReadVersion, Txns)] ++
        %% An acknowledged transaction of the same batch, after the writer.
        [
            {missed_in_batch, maps:with([client, epoch, version, index], T)}
         || #{Writer := #{version := Version, index := Index}} <- [ByClient],
            Version =:= Written,
            T = #{index := Later} <- changers(Key, Written - 1, Written, Txns),
            Later > Index
        ].

%% The acknowledged transactions that wrote or cleared `Key' at a commit
%% version above `After' and at or below `UpTo'.
changers(Key, After, UpTo, Txns) ->
    [
        T
     || T = #{version := Version, writes := Writes, clears := Clears} <- Txns,
        Version > After,
        Version =< UpTo,
        lists:member(Key, Writes) orelse lists:member(Key, Clears)
    ].

%% The coordinated state has the end version of each ended epoch. The
%% first version of the next epoch is the end version plus the gap.
storage_rollback(Tab, Storages, CState) ->
    %% The checks read every write of each StorageServer, so the harness
    %% performs them only when a StorageServer or the coordinated state
    %% has changed.
    Observed = [observe(Storage) || Storage <- Storages],
    Old = maps:get(old, cstate(CState), []),
    Key = erlang:phash2({Observed, Old}),
    case ets:lookup(Tab, storage_checked) of
        [{_, Key}] ->
            ok;
        _ ->
            Checks =
                [fun() -> storage_rollback(One, Old) end || One <- Observed] ++
                    [fun() -> replicas_agree(A, B, Old) end || {I, A} <- lists:enumerate(Observed), {J, B} <- lists:enumerate(Observed), I < J],
            case first_violation(Checks) of
                ok ->
                    true = ets:insert(Tab, {storage_checked, Key}),
                    ok;
                Violation ->
                    Violation
            end
    end.

%% The published state of a StorageServer, with every version of every
%% key of its storage engine in the key `data'. The StorageServer
%% publishes the view of its store, which has no private table. See
%% `view/1' of `redwall_storage_ets'. The store of the engine
%% `ets' does not exist after the kill of the StorageServer.
%%
%% Each key of `data' is the canonical form of the key of the
%% StorageServer (`redwall_dst_client:canon/1'). A StorageServer has the
%% key of the last write that it applied, so two StorageServers can have
%% two forms of one key.
observe(Storage) ->
    case eta_observe:read(Storage) of
        Published = #{engine := Engine, view := View} ->
            try
                Data = redwall_storage:versions(#{engine => Engine, store => View}),
                Published#{data => maps:fold(fun(Key, Versions, Acc) -> Acc#{redwall_dst_client:canon(Key) => Versions} end, #{}, Data)}
            catch
                error:badarg -> no_table
            end;
        Other ->
            Other
    end.

%% Two StorageServers have the same writes for each key, for the versions
%% above the oldest version of both, up to the lower one of the two
%% versions. A write that a recovery discarded is not compared.
replicas_agree(#{version := VersionA, oldest := OldestA, data := DataA}, #{version := VersionB, oldest := OldestB, data := DataB}, Old) ->
    Low = max(OldestA, OldestB),
    High = min(VersionA, VersionB),
    Ends = [End || #{end_version := End} <- Old, End > 0],
    Compared = fun(Data) ->
        maps:filtermap(
            fun(_Key, Versions) ->
                case [Write || Write = {Written, _Change} <- Versions, Written > Low, Written =< High, not lists:any(fun(End) -> Written > End andalso Written < End + ?EPOCH_GAP end, Ends)] of
                    [] -> false;
                    Writes -> {true, Writes}
                end
            end,
            Data
        )
    end,
    {A, B} = {Compared(DataA), Compared(DataB)},
    case A =:= B of
        true ->
            ok;
        false ->
            Keys = [Key || Key <- lists:usort(maps:keys(A) ++ maps:keys(B)), maps:get(Key, A, []) =/= maps:get(Key, B, [])],
            Key = hd(Keys),
            violation(replicas_agree, #{key => Key, first => maps:get(Key, A, []), second => maps:get(Key, B, []), above => Low, up_to => High})
    end;
replicas_agree(_NoEventYetOrNoTable, _Other, _Old) ->
    ok.

storage_rollback(Observed, Old) ->
    case Observed of
        #{version := Version, data := Data} ->
            Ends = [End || #{end_version := End} <- Old, End > 0, Version >= End + ?EPOCH_GAP],
            Kept = [
                {Key, Written, End}
             || End <- Ends,
                {Key, Versions} <- maps:to_list(Data),
                {Written, _Value} <- Versions,
                Written > End,
                Written < End + ?EPOCH_GAP
            ],
            case Kept of
                [] -> ok;
                [{Key, Written, End} | _] -> violation(storage_rollback, #{key => Key, written => Written, end_version => End, storage_version => Version})
            end;
        _NoEventYet ->
            ok
    end.

kcv_below_dv(Logs) ->
    ByEpoch = lists:foldl(
        fun({{Epoch, _Index}, #{dv := DV, kcv := KCV}}, Acc) ->
            maps:update_with(Epoch, fun({MaxKCV, MinDV}) -> {max(MaxKCV, KCV), min(MinDV, DV)} end, {KCV, DV}, Acc)
        end,
        #{},
        Logs
    ),
    case [{Epoch, KCV, DV} || {Epoch, {KCV, DV}} <- maps:to_list(ByEpoch), KCV > DV] of
        [] -> ok;
        [{Epoch, KCV, DV} | _] -> violation(kcv_below_dv, #{epoch => Epoch, kcv => KCV, dv => DV})
    end.

chain(Logs) ->
    case [Id || {Id, #{records := Records}} <- Logs, not chained(gb_trees:to_list(Records))] of
        [] -> ok;
        [Id | _] -> violation(chain, #{log => Id})
    end.

%% The first record of a LogServer has any previous LSN.
chained([{LSN, _} | Rest = [{_Next, {Prev, _Body}} | _]]) ->
    Prev =:= LSN andalso chained(Rest);
chained(_) ->
    true.

first_violation([]) ->
    ok;
first_violation([Check | Rest]) ->
    case Check() of
        ok -> first_violation(Rest);
        Violation -> Violation
    end.

%% Every violation has a `property' key. `eta_shrink' compares two
%% failures by this key.
violation(Property, Detail) ->
    {violation, Detail#{property => Property}}.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

spawn_op(Node, Fun, S = #{ops := Ops}) ->
    Pid = eta_run:spawn_op(Fun),
    ok = eta_net:place(Node, [Pid]),
    S#{ops := Ops ++ [Pid]}.

%% Element `R' of the list, counted in a circle, or `none'.
pick(_R, []) -> none;
pick(R, List) -> lists:nth(R rem length(List) + 1, List).

%% Starts the coordinator `Id' on its node.
start_coordinator(Id = {_Name, Node}, S = #{mode := Mode}) ->
    Dir = disk_dir(),
    spawn_op(Node, fun() -> redwall_dst_client:coordinator(Id, Dir, Mode) end, S).

%% Starts each killed coordinator again.
restart_coordinators(S = #{tab := Tab, cstate := {peace, Tab, Ids}}) ->
    lists:foldl(
        fun(Id, Acc) ->
            case ets:take(Tab, {coordinator_down, Id}) of
                [_Row] ->
                    bump(restarts_coordinator),
                    start_coordinator(Id, Acc);
                [] ->
                    Acc
            end
        end,
        S,
        Ids
    );
restart_coordinators(S) ->
    S.

%% The directory of the data of the coordinators. Every run uses this one
%% directory, so that the name of the directory is the same in every run.
%% The harness empties the directory before and after each run. The
%% environment variable `REDWALL_DST_DISK_DIR' sets the directory.
%% `dst/run.sh' gives each test a directory of its own, because the tests
%% run at the same time.
disk_dir() ->
    case os:getenv("REDWALL_DST_DISK_DIR") of
        false -> filename:join(filename:basedir(user_cache, "redwall_dst"), "disk");
        Dir -> Dir
    end.

%% Deletes the files and the directories of a directory. The function
%% uses `prim_file', because the file server is a process that no schedule
%% controls.
clear_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {ok, Names} ->
            lists:foreach(
                fun(Name) ->
                    Path = filename:join(Dir, Name),
                    case prim_file:delete(Path) of
                        ok ->
                            ok;
                        {error, _} ->
                            ok = clear_dir(Path),
                            ok = prim_file:del_dir(Path)
                    end
                end,
                Names
            );
        {error, enoent} ->
            ok
    end.

%% Reads the coordinated state that the process publishes. The read sends
%% no message to the process.
cstate({local, CState}) ->
    case eta_observe:read(CState) of
        {State, _Cas} when is_map(State) -> State;
        _ -> #{}
    end;
%% The state with the highest index that a server of the peace cluster has
%% applied. See `redwall_dst_machine'.
cstate({peace, Tab, _Ids}) ->
    case ets:lookup(Tab, cstate) of
        [{cstate, _Index, State}] -> State;
        [] -> #{}
    end.

current(CState) ->
    maps:get(current, cstate(CState), undefined).

%% The LogServers that run, from the table of the run.
log_pids(Tab) ->
    lists:sort([{Id, Pid} || [Id, Pid] <- ets:match(Tab, {{log, '$1'}, '$2'}), is_process_alive(Pid)]).

%% The published state of each LogServer that runs and has handled at least
%% one event.
logs(Tab) ->
    [{Id, State#{records := Tree, pid => Pid}} || {Id, Pid} <- log_pids(Tab), State <- [eta_observe:read(Pid)], is_map(State), is_map_key(records, State), Tree <- records(State)].

%% The records of a LogServer as a tree from the LSN to the previous LSN
%% and the body. The LogServer keeps the records in an ETS table. The
%% result is the empty list if the table does not exist.
records(#{records := Table}) ->
    try ets:tab2list(Table) of
        Rows -> [gb_trees:from_orddict([{LSN, {Prev, Body}} || {LSN, Prev, Body} <- Rows])]
    catch
        error:badarg -> []
    end.

%% Kills a worker node, if the kill leaves every epoch with at most one
%% killed LogServer. An epoch keeps a record on 2 of its 3 LogServers, so
%% a second kill can remove an acknowledged record.
kill_worker(Node, Role, S = #{tab := Tab}) ->
    Epochs = lists:usort([Epoch || [{Epoch, _Index}, Pid] <- ets:match(Tab, {{log, '$1'}, '$2'}), [] =/= ets:match(Tab, {{at, Pid}, Node, log})]),
    case lists:any(fun(Epoch) -> ets:member(Tab, {killed_log, Epoch}) end, Epochs) of
        true ->
            S;
        false ->
            true = ets:insert(Tab, [{{killed_log, Epoch}, true} || Epoch <- Epochs]),
            Candidate =
                case ets:lookup(Tab, {controller, Node}) of
                    [{_, Controller}] -> is_process_alive(Controller);
                    [] -> false
                end,
            _ = Candidate andalso bump(kills_controller),
            [{_, _Worker, Class}] = ets:lookup(Tab, {worker, Node}),
            true = ets:insert(Tab, {{worker_down, Node}, {Class, Candidate}}),
            bump(kills_worker),
            kill(Node, list_to_atom("kills_" ++ atom_to_list(Role)), S)
    end.

%% Kills every process on a simulated node, between steps.
kill(Node, Stat, S) ->
    ok = eta_net:kill_node(Node),
    bump(Stat),
    S.

%% Kills the processes at the end of a run and waits until each process is
%% dead. The scheduler has released every process by then, so a watcher
%% handles its `DOWN' messages at once.
kill_all(Pids) ->
    Refs = [{Pid, erlang:monitor(process, Pid)} || Pid <- Pids],
    lists:foreach(fun({Pid, _Ref}) -> exit(Pid, kill) end, Refs),
    lists:foreach(
        fun({Pid, Ref}) ->
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            end
        end,
        Refs
    ).
