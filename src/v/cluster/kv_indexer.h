/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "cluster/fwd.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "storage/fwd.h"
#include "storage/ntp_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <filesystem>
#include <memory>
#include <optional>

namespace cluster {

/// Maintains the key index (storage::kv_index) of one partition replica.
///
/// Only records below the last stable offset are indexed, and records of
/// aborted transactions are skipped, so the index never points at a record
/// a read_committed consumer could not see. Raft does not truncate committed
/// offsets, so the index is only ever appended to. It covers a contiguous
/// range of the local log: lookups miss keys last written before that range.
class kv_indexer {
public:
    kv_indexer(raft::consensus*, ss::shared_ptr<rm_stm>);
    ~kv_indexer();

    /// Opens the index and starts following the log in the background.
    ss::future<> start();
    ss::future<> stop();

    /// Kafka offset of the latest committed record carrying `key`.
    ss::future<std::optional<kafka::offset>> lookup(bytes_view key);

    /// Log offset the next indexing pass starts from; everything committed
    /// below it is reflected in the index.
    model::offset next_offset() const { return _next; }

    /// False from start, and from a rebuild, until a pass has read the log
    /// up to the last stable offset. Until then lookups miss keys that the
    /// log does hold.
    bool caught_up() const { return _caught_up; }

    static ss::future<> remove_index(const storage::ntp_config&);

private:
    void setup_metrics();
    ss::future<> run_loop();
    ss::future<> index_committed();
    /// The log start moved past records that were never indexed (snapshot
    /// installed on a follower, local retention). Entries indexed before the
    /// gap may have been superseded inside it, so the index cannot be kept.
    bool has_gap() const;
    ss::future<> reset_index();

    raft::consensus* _raft;
    ss::shared_ptr<rm_stm> _rm_stm;
    std::unique_ptr<storage::kv_index> _index;
    model::offset _next;
    bool _caught_up{false};
    uint64_t _rebuilds{0};
    ss::abort_source _as;
    ss::gate _gate;
    metrics::internal_metric_groups _metrics;
};

} // namespace cluster
