# Topic key index (redpanda.kv.index.enabled)

Status: proof of concept, builds and passes its unit tests on dev; branch: https://github.com/sonirico/redpanda/tree/plan/redpanda-kv-index-poc (staging PR: https://github.com/sonirico/redpanda/pull/1)

- Feature Name: topic_key_index
- Status: draft
- Start Date: 2026-08-17
- Authors: Marcos Benedicto
- Issue: none (discussion: https://github.com/redpanda-data/redpanda/discussions/31597)

# Executive Summary

## What is being proposed

A per-partition secondary index, `storage::kv_index`, that maps a
record's raw key to the Kafka offset of the latest record carrying that
key (values are never copied; a lookup reads the record back from the
log), for topics that opt in via the `redpanda.kv.index.enabled` topic
property, and a read-through lookup endpoint, `GET /kv/{topic}/{key}`,
exposed on Pandaproxy.

## Why (short reason)

Looking up the latest value for a key currently requires scanning a
compacted topic, and a scan that reaches segments evicted to tiered
storage is paid against the object store.

## How (short plan)

Maintain an lsm-backed key-to-offset index of the committed records of
locally compacted topics, on every replica, at `<ntp dir>/kv_index/`.
A per-partition fiber follows the local log up to the last stable
offset and skips aborted transactions.
Serve lookups in-process on Pandaproxy by resolving the partition,
reading the index on the owning shard, and reading the record back
through `kafka::partition_proxy::make_reader`. Records whose local
segments were evicted by tiered storage are served only when the
operator opts in via the `kv_index_remote_read_enabled` cluster
property; otherwise the lookup returns 404.

## Impact

Enables sub-millisecond, in-broker key lookups for locally compacted
topics that opt in, without a full log scan. The index is optional per topic, gated behind a feature flag and a
cluster property, and defaults to off. Every replica maintains its own
index, so read load can be served from whichever node hosts the queried
partition, subject to that replica's index coverage and replication lag
(see corner cases); lookups against a non-hosting node return 421 rather
than being forwarded.

# Motivation
## Why are we doing this?

A compacted topic is commonly used as a table, but the broker has no
way to locate a key without scanning the log, so any reader that wants
the latest value for one key pays for all of them, and a key that only
exists in segments evicted to tiered storage costs a scan through the
remote read path.

## What use cases does it support?

Point lookups of the latest value for a known key in a compacted topic,
without scanning the log.

## What is the expected outcome?

Lower latency point lookups for compacted topics that opt in, served
entirely in-broker, with tiered-storage reads for evicted keys as an
explicit operator opt-in.

# Guide-level explanation
## How do we teach this?

A user enables the index on a compacting topic by setting the topic
property `redpanda.kv.index.enabled` to `true`, either at topic creation
or via alter-configs / incremental-alter-configs. This requires the
`kv_index` feature to be active, the cluster property
`kv_index_enabled` to be `true` (both default to disabled), and the
topic's `cleanup.policy` to include `compact`. The current value is
visible via describe-configs.

Once enabled, every replica of the topic maintains a local index from
key to the log offset of the latest record carrying that key. A client
looks up a key with:

```
GET /kv/{topic}/{key}[?partition=N]
```

against Pandaproxy. If `partition` is omitted, the partition is derived
from the key as the default partitioner of the Java client, librdkafka's
murmur2 partitioner and franz-go do: murmur2 of the key with the sign
bit cleared, modulo the partition count.
The response is the raw record value with `X-Kv-Partition` and
`X-Kv-Offset` headers on success (200), or 404 if the key is not found,
is a tombstone, the topic is not indexed, or the record's local segment
was evicted to tiered storage and `kv_index_remote_read_enabled` is
false. While the replica's index is still catching up with its log the
answer is 503, since a miss would not mean the key is absent. A request
that lands on a broker that does not host the queried partition returns
421.

## Introducing new named concepts.

- `redpanda.kv.index.enabled`: topic property, `std::optional<bool>`,
  default false, requests the key index for a compacting topic.
- `kv_index_enabled`: cluster property, default false, cluster-wide
  switch gating the topic property. Partitions read it when they start
  and when their topic properties change, so it is marked as needing a
  restart.
- `kv_index_remote_read_enabled`: cluster property, default false, allows a
  lookup to read a record whose local segment was evicted to tiered
  storage; when false such lookups return 404.
- `kv_index`: feature flag gating the topic property.
- `storage::kv_index`: the per-partition on-disk index implementation.

# Reference-level explanation

## Interaction with other features

- Requires `cleanup.policy` to include `compact`; validated at topic
  creation by `kv_index_create_validator`
  (`src/v/kafka/server/handlers/topics/validators.h`) and on every
  property update by `kv_index_validator`
  (`src/v/cluster/topic_validators.h`), which runs on the merged
  properties in the topic table. An update that enables the index on a
  non-compacting topic, or removes compaction from an indexed one, is
  rejected. The policy has to be set on the topic; one inherited from
  the cluster default is not accepted.
- Interacts with tiered storage: a lookup whose Kafka offset is below
  the local start offset would read through
  `kafka::partition_proxy::make_reader` into the remote read path, which
  is a cold random-access read against the object store (one remote
  segment chunk fetched into the cloud storage cache per miss). That
  read only happens when `kv_index_remote_read_enabled` is true, the
  topic has `redpanda.remote.read` enabled and the offset is still
  retained remotely; otherwise the lookup returns 404.
- Property propagation follows the same plumbing as the existing
  `schema_registry_context`-style topic properties: create-topic,
  alter-configs, incremental-alter-configs, describe-configs,
  cluster-link property propagation, the offline log viewer, and
  `partition::update_configuration`.

## Telemetry & Observability

- `vectorized_cluster_kv_index_next_offset`, per indexed partition
  replica: the log offset the next indexing pass starts from. Its
  distance to `vectorized_cluster_partition_last_stable_offset` is the
  indexing lag.
- `vectorized_cluster_kv_index_rebuilds`, per indexed partition replica:
  how many times the index was dropped and rebuilt.
- Lookups get request latency and status code metrics from the
  Pandaproxy route probe, like every other endpoint.

## Corner cases dissected by example.

- Stale pointer after compaction: compaction removes a record once a
  later one replaces it, and it may do so before the indexer has read
  the later one. The read path checks that the record at the indexed
  offset exists and carries the requested key; if not, the index is
  behind and the answer is 503, not 404.
- Tombstones: a tombstone record (no value) is removed from the index,
  so a lookup for a tombstoned key returns 404.
- Compressed batches: batches are decompressed before indexing and
  again before the matching record is extracted on the read path.
- Follower-served lookups: the index is maintained on all replicas, so
  any replica can serve a lookup for a partition it hosts, but a
  follower may answer with an older value than the leader.
  A broker that does not host the queried partition returns 421.
- Coverage: a replica's index covers one contiguous range of its local
  log, from the log start when indexing began up to the last stable
  offset. A replica created by a partition move that recovered from a
  snapshot, or a topic that enabled the index after local eviction,
  does not index the keys last written before that range, and answers
  404 for them. This only shows on topics whose local log is truncated:
  tiered storage, or `compact,delete`.
- Log start passing the indexer: when a follower installs a snapshot, or
  local retention removes records the indexer had not read, a key may
  have been updated or deleted in the part that was skipped. The entry
  indexed before it would then point at a superseded record, so the
  index is dropped and rebuilt from the new log start.
- Index still catching up: after a broker start, after a rebuild, and
  after enabling the index on a topic that already holds data, lookups
  answer 503 until one pass has read the log up to the last stable
  offset.
- Uncommitted records and open transactions: only records below the
  last stable offset are indexed, so a write that is not yet committed,
  or belongs to a transaction still open, leaves the index pointing at
  the previous committed value. A lookup returns that value, as a
  read_committed consumer would see it.
- Aborted transactions: batches of aborted transactions are skipped, so
  they never replace the pointer to a committed value.
- Lag: the index follows commits asynchronously, so a lookup right after
  an acknowledged write may still return the previous value.
- Custom partitioners: since the endpoint's own partition resolution
  only knows the default murmur2 partitioner, callers that produced
  with a different partitioner must pass `?partition=N` explicitly.

## Detailed design - What needs to change to get there

- `storage::kv_index` (`src/v/storage/kv_index.h`): a per-partition
  index backed by one `lsm::database` per partition, stored under
  `<ntp dir>/kv_index/`. `options` control `dir`, `write_buffer_size`
  (default 1 MiB), `memtable_budget`, `block_cache_size` (default
  1 MiB), and `max_open_files` (default 64). The indexes of a shard
  share `memtable_budget`, 5% of the shard's memory by default: an
  index flushes once its unflushed entries pass an equal share of it,
  with a floor of 64 KiB. There is no WAL; the stored value is
  the record's Kafka offset, translated at index time; `last_applied()`
  reports the highest durably indexed log offset so that opening the
  index resumes indexing from `last_applied()+1`.
- `cluster::kv_indexer` (`src/v/cluster/kv_indexer.h`): owns the index
  of one partition replica and runs a background fiber. Each pass waits
  for the raft commit index to advance and for `rm_stm` to apply it,
  reads the local log from where it left off up to the offset before
  the last stable offset, drops batches that fall in an aborted
  transaction range of their producer, and indexes the rest. It drops
  and rebuilds the index when the log start has moved past the next
  offset to read, and reports whether it has caught up.
- `cluster::partition` (`src/v/cluster/partition.cc`): starts the
  indexer when `kv_index_wanted()` (the topic property is requested,
  the log is locally compacted, and the cluster property is enabled),
  stops it on partition stop, starts or stops it from
  `update_configuration`, and removes the index directory when the
  index is disabled or the partition is removed. `kv_indexer()` exposes
  the current indexer, if any.
- `pandaproxy::rest` (`src/v/pandaproxy/rest/kv_handlers.cc`):
  `GET /kv/{topic}/{key}` resolves the partition (murmur2 of the key
  when `partition` is not given), locates the owning shard via
  `shard_table` and `partition_manager`, answers 503 if that
  partition's `cluster::kv_indexer` has not caught up, looks the key up
  in it on the owning shard, rejects
  offsets below the local
  start unless `kv_index_remote_read_enabled` is set, and reads the
  record through `kafka::partition_proxy::make_reader`.

## Detailed design - How it works

Indexing is off the write path. Every replica of a locally compacted
topic with `redpanda.kv.index.enabled` set runs a `kv_indexer` that
follows its own log and records the offset of the latest committed
record for each key. Because there is no WAL, the index only durably
reflects offsets it has flushed; on open, the indexer compares
`last_applied()` against the log's dirty offset and, if the index is
ahead of the log (for example after an unclean shutdown that lost
acknowledged data), rebuilds the index from the log rather than
trusting a partially-written state. Raft never truncates committed
offsets, so there is no rebuild on suffix truncation. Removing the
partition removes the index directory.

On the read path, Pandaproxy resolves the partition for the requested
key, dispatches to the shard that hosts it, and performs the index
lookup and the subsequent record read on that shard in-process, without
a network hop to another broker. The offset stored in the index is
already a Kafka offset, translated when the batch was indexed, so it can
be handed to `kafka::partition_proxy` even after the local offset
translator was prefix-truncated by tiered storage eviction. Whether the
read may fall through to the remote read path is decided by the
`kv_index_remote_read_enabled` gate described above.

## Drawbacks

- Memory: one memtable entry takes about 900 bytes besides its key
  (measured the same on a fastbuild and on a release build), far more
  than lsm counts. Unflushed entries are bounded per shard by
  `memtable_budget`, or by 64 KiB per indexed partition when there are
  more partitions than the budget can give that much to. Each index
  also has a block cache of up to 1 MiB, filled by lookups, and its own
  open files. Measured on a fastbuild broker with one 4 GiB shard, 1000
  indexed partitions and 2 million keys: 456 MiB more than idle with
  the budget, 2077 MiB without it.
- Many small flushes: with thousands of indexed partitions on a shard
  each index flushes every few dozen entries.
- Catch-up cost on restart: if the index was not durably flushed before
  a restart, the indexer rescans the un-flushed tail of the log, and
  lookups answer 503 until it is done.
- Enabling the index on an existing topic scans its whole local log in
  the background.
- The lookup endpoint is a data-plane read exposed on the Pandaproxy
  port without quotas, unlike other data-plane paths.

## Rationale and Alternatives

- Why is this design the best in the space of possible designs?
  It reuses the existing storage and Pandaproxy layers, stores offsets
  only (so the log remains the single source of truth and the index size
  is bounded by the live key set, not by value size), keeps the index
  local to the partition it describes (so it moves with the partition on
  reassignment without extra replication logic), and piggybacks on the
  existing tiered-storage read path instead of duplicating it.
- What other designs have been considered?
  An admin API v2 ConnectRPC service and a Kafka protocol extension
  were considered. Indexing from the append path was prototyped first
  and dropped: with one offset per key, an uncommitted or aborted write
  replaced the pointer to the committed value.
- What is the impact of not doing this?
  Users needing key lookups continue to scan compacted topics
  themselves.

## Unresolved questions

- Cross-node redirect: whether a lookup against a broker that does not
  host the partition should be forwarded rather than returning 421.
- Quotas/limits on the lookup endpoint.
- Whether the index should be replicated as part of the log's
  replication protocol, or rebuilt independently on each replica as it
  is in the proof of concept.
- Exposing the index lookup over the Kafka protocol instead of, or in
  addition to, the Pandaproxy REST endpoint.
- Scale: one `lsm::database` per indexed partition was exercised with
  1000 partitions per shard, not more, and only for memory.
- Lookup latency has one data point: a release build, three brokers
  with one core each on one machine, one partition holding 400k keys,
  one client sending 40k lookups for random keys over a single
  connection. Median 0.64 ms, p99 0.95 ms. Nothing under concurrency or
  with produce load.
- One run of 16 concurrent lookup threads against a topic being
  rewritten got 2528 empty 404 answers out of 107117 for keys that
  existed. Eight more runs, some replaying the same sequence of
  restarts, got none. The cause is not known.
- Tiered storage: the remote read path and local retention against the
  indexer have unit coverage for the index gap only; the ducktape tests
  that cover them could not be run where this was developed.
- Records produced through Pandaproxy use Redpanda's own client
  partitioner, which does not clear the sign bit of the hash. For those
  the endpoint derives a different partition unless the partition count
  is a power of two; `?partition=N` works.
