# Topic key index (redpanda.kv.index.enabled)

Status: proof of concept, not yet compiled against a full toolchain; branch: https://github.com/sonirico/redpanda/tree/plan/redpanda-kv-index-poc (staging PR: https://github.com/sonirico/redpanda/pull/1)

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

Looking up the latest value for a key currently requires either scanning
a compacted topic or running a fetch-path sidecar outside the broker.
Both pay a full scan to locate the key, and a scan that reaches segments
evicted to tiered storage is paid against the object store.

## How (short plan)

Maintain an lsm-backed key-to-offset index from the append path of
locally compacted topics, on every replica, at `<ntp dir>/kv_index/`.
Serve lookups in-process on Pandaproxy by resolving the partition,
reading the index on the owning shard, and reading the record back
through `kafka::partition_proxy::make_reader`. Records whose local
segments were evicted by tiered storage are served only when the
operator opts in via the `kv_index_remote_read_enabled` cluster
property; otherwise the lookup returns 404.

## Impact

Enables sub-millisecond, in-broker key lookups for locally compacted
topics that opt in, without requiring an external sidecar or a full log
scan. The index is optional per topic, gated behind a feature flag and a
cluster property, and defaults to off. Every replica maintains its own
index, so read load can be served from whichever node hosts the queried
partition, subject to that replica's index coverage and replication lag
(see corner cases); lookups against a non-hosting node return 421 rather
than being forwarded.

# Motivation
## Why are we doing this?

The fetch-path sidecar `github.com/sonirico/rpkv` (contract suite and
benchmarks: p50 11.07 ms / p99 16.85 ms loopback, 100k keys, see
`docs/benchmarks/read-latency.md`) shows there is demand for key lookups
and establishes the latency floor of any out-of-broker design. An
out-of-broker sidecar also has no way to locate a key without scanning,
so a key that only exists in segments evicted to tiered storage costs a
scan through the remote read path.

## What use cases does it support?

Point lookups of the latest value for a known key in a compacted topic,
without scanning the log or running a separate lookup service.

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
from the key using the default murmur2 partitioner, matching the
partition a producer using the same partitioner would have written to.
The response is the raw record value with `X-Kv-Partition` and
`X-Kv-Offset` headers on success (200), or 404 if the key is not found,
is a tombstone, the topic is not indexed, or the record's local segment
was evicted to tiered storage and `kv_index_remote_read_enabled` is
false. A request that lands on a broker that does not host the queried
partition returns 421.

## Introducing new named concepts.

- `redpanda.kv.index.enabled`: topic property, `std::optional<bool>`,
  default false, requests the key index for a compacting topic.
- `kv_index_enabled`: cluster property, default false, cluster-wide
  switch gating the topic property.
- `kv_index_remote_read_enabled`: cluster property, default false, allows a
  lookup to read a record whose local segment was evicted to tiered
  storage; when false such lookups return 404.
- `kv_index`: feature flag gating the topic property.
- `storage::kv_index`: the per-partition on-disk index implementation.

# Reference-level explanation

## Interaction with other features

- Requires `cleanup.policy` to include `compact`; validated at topic
  creation and at alter-configs time by `kv_index_create_validator`
  (`src/v/kafka/server/handlers/topics/validators.h`).
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

None in the proof of concept. This is an unresolved question.

## Corner cases dissected by example.

- Stale pointer after compaction: if the index points at an offset that
  compaction has since removed or overwritten, the read path re-checks
  key equality against the record actually read; a mismatch is treated
  as not found and returns 404.
- Tombstones: a tombstone record (no value) is removed from the index,
  so a lookup for a tombstoned key returns 404.
- Compressed batches: batches are decompressed before indexing and
  again before the matching record is extracted on the read path.
- Follower-served lookups: the index is maintained on all replicas, so
  any replica can serve a lookup for a partition it hosts, but a
  follower may answer with an older value than the leader, and a
  replica's index only covers records that were in its local log when
  the index was enabled plus records appended since; a replica created
  by a partition move that recovered from a snapshot, or a topic that
  enabled the index after local eviction, does not index the older keys.
  A broker that does not host the queried partition returns 421.
- Uncommitted records: the index is fed at append time, before raft
  commit; a lookup whose offset is at or beyond the high watermark
  returns 404 instead of exposing a record that may still be truncated.
- Transactions: records from aborted transactions are indexed like any
  other and are not filtered on the read path; this is listed under
  unresolved questions.
- Custom partitioners: since the endpoint's own partition resolution
  only knows the default murmur2 partitioner, callers that produced
  with a different partitioner must pass `?partition=N` explicitly.

## Detailed design - What needs to change to get there

- `storage::kv_index` (`src/v/storage/kv_index.h`): a per-partition
  index backed by one `lsm::database` per partition, stored under
  `<ntp dir>/kv_index/`. `options` control `dir`, `write_buffer_size`
  (default 1 MiB), `block_cache_size` (default 1 MiB), and
  `max_open_files` (default 64). There is no WAL; the stored value is
  the record's Kafka offset, translated at index time; `last_applied()`
  reports the highest durably indexed log offset so that opening the
  index resumes indexing from `last_applied()+1`.
- `disk_log_impl` (`src/v/storage/disk_log_impl.cc`): opens the index
  when `kv_index_wanted()` (the topic property is requested, the log is
  locally compacted, and the cluster property is enabled), closes it on
  log close/remove, rebuilds it via `rebuild_kv_index()` when a suffix
  truncation point is at or before the last indexed offset, and indexes
  every appended batch from `disk_log_appender` via
  `kv_index_batch()`. `notify_kv_index_update()` re-evaluates whether
  the index is wanted and opens or closes it accordingly; `get_kv_index()`
  exposes the current index, if any.
- `pandaproxy::rest` (`src/v/pandaproxy/rest/kv_handlers.cc`):
  `GET /kv/{topic}/{key}` resolves the partition (murmur2 of the key
  when `partition` is not given), locates the owning shard via
  `shard_table` and `partition_manager`, looks the key up in that
  partition's `storage::kv_index` on the owning shard, rejects offsets
  at or beyond the high watermark, rejects offsets below the local
  start unless `kv_index_remote_read_enabled` is set, and reads the
  record through `kafka::partition_proxy::make_reader`.

## Detailed design - How it works

On the write path, every replica of a locally compacted topic with
`redpanda.kv.index.enabled` set indexes each appended batch as it is
written, recording the offset of the latest record for each key seen.
Because there is no WAL, the index only durably reflects offsets it has
flushed; on open, `disk_log_impl` compares `last_applied()` against the
log's dirty offset and, if the index is ahead of the log (for example
after an unclean shutdown), rebuilds the index from the log rather than
trusting a partially-written state. Suffix truncation similarly
triggers a rebuild whenever the truncation point is at or before the
last indexed offset, since the index may reference offsets that no
longer exist. Removing the partition removes the index directory.

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

- Memory per partition: each open index carries roughly a 1 MiB
  memtable and a 1 MiB block cache, plus its own open file descriptors,
  for every partition of every topic that enables the index.
- Rebuild cost on restart: if the index was not durably flushed before
  a restart, it is rebuilt from the log before it can serve lookups,
  which is a scan of the un-flushed tail of the log.
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
  An admin API v2 ConnectRPC service, a Kafka protocol extension, and
  an external sidecar (the existing `github.com/sonirico/rpkv`) were
  considered. The sidecar approach was already prototyped and
  benchmarked; its latency floor and inability to serve evicted keys
  without a broker fetch motivated the in-broker approach described
  here.
- What is the impact of not doing this?
  Users needing key lookups continue to rely on the out-of-broker
  sidecar or on scanning compacted topics themselves.

## Unresolved questions

- Cross-node redirect: whether a lookup against a broker that does not
  host the partition should be forwarded rather than returning 421.
- Quotas/limits on the lookup endpoint.
- Metrics and other telemetry for the index and the lookup path.
- Whether the index should be replicated as part of the log's
  replication protocol, or rebuilt independently on each replica as it
  is in the proof of concept.
- Exposing the index lookup over the Kafka protocol instead of, or in
  addition to, the Pandaproxy REST endpoint.
- Whether the lookup path should filter records from aborted transactions
  as the fetch path does.
