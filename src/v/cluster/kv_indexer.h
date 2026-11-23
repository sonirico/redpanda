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
/// offsets, so the index is only ever appended to.
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

    static ss::future<> remove_index(const storage::ntp_config&);

private:
    ss::future<> run_loop();
    ss::future<> index_committed();

    raft::consensus* _raft;
    ss::shared_ptr<rm_stm> _rm_stm;
    std::unique_ptr<storage::kv_index> _index;
    model::offset _next;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace cluster
