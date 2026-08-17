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
#include "base/units.h"
#include "bytes/bytes.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

namespace storage {

/// Per-partition secondary index mapping raw record keys to the log offset
/// of the latest record carrying that key.
///
/// Backed by lsm::database (no WAL) with disk persistence under
/// options::dir; rebuildable from the log: after open, last_applied() is
/// the highest log offset durably indexed and callers resume indexing from
/// the next offset.
class kv_index {
public:
    struct options {
        std::filesystem::path dir;
        size_t write_buffer_size = 1_MiB;
        size_t block_cache_size = 1_MiB;
        uint32_t max_open_files = 64;
    };

    static ss::future<std::unique_ptr<kv_index>> open(options);

    ss::future<> index_batch(const model::record_batch&);

    ss::future<std::optional<model::offset>> lookup(bytes_view key);

    model::offset last_applied() const;

    ss::future<> flush();

    ss::future<> close();

    static ss::future<> remove(std::filesystem::path dir);

private:
    kv_index(lsm::database, model::offset last_applied);

    lsm::database _db;
    model::offset _last_applied;
    ss::gate _gate;
};

} // namespace storage
