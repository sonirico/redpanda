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

#include "storage/kv_index.h"

#include "bytes/iobuf_parser.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/persistence.h"
#include "model/batch_compression.h"
#include "ssx/clock.h"
#include "ssx/time.h"

#include <seastar/core/byteorder.hh>
#include <seastar/util/file.hh>

namespace storage {

kv_index::kv_index(lsm::database db, model::offset last_applied)
  : _db(std::move(db))
  , _last_applied(last_applied) {}

ss::future<std::unique_ptr<kv_index>> kv_index::open(options o) {
    lsm::options lo;
    lo.write_buffer_size = o.write_buffer_size;
    lo.block_cache_size = o.block_cache_size;
    lo.max_open_files = o.max_open_files;
    lsm::io::persistence p{
      .data = co_await lsm::io::open_disk_data_persistence(o.dir),
      .metadata = co_await lsm::io::open_disk_metadata_persistence(o.dir),
    };
    auto db = co_await lsm::database::open(std::move(lo), std::move(p));
    auto max_seqno = db.max_applied_seqno();
    auto last_applied = max_seqno ? model::offset(
                                      static_cast<int64_t>(max_seqno->value()))
                                  : model::offset{};
    co_return std::unique_ptr<kv_index>(
      new kv_index(std::move(db), last_applied));
}

ss::future<> kv_index::index_batch(const model::record_batch& b) {
    auto holder = _gate.hold();
    if (
      b.header().type != model::record_batch_type::raft_data
      || b.header().attrs.is_control()) {
        co_return;
    }
    const model::record_batch* src = &b;
    std::optional<model::record_batch> decompressed;
    if (b.compressed()) {
        decompressed = co_await model::decompress_batch(b);
        src = &*decompressed;
    }
    auto wb = _db.create_write_batch();
    size_t ops = 0;
    model::offset last = _last_applied;
    src->for_each_record([&](const model::record& r) {
        if (!r.has_key()) {
            return;
        }
        model::offset o = src->base_offset() + model::offset(r.offset_delta());
        if (o <= _last_applied) {
            return;
        }
        bytes k = iobuf_to_bytes(r.key());
        std::string_view sv(reinterpret_cast<const char*>(k.data()), k.size());
        if (r.is_tombstone()) {
            wb.remove(sv, lsm::sequence_number(o()));
        } else {
            iobuf v;
            auto be = ss::cpu_to_be(static_cast<uint64_t>(o()));
            v.append(reinterpret_cast<const char*>(&be), 8);
            wb.put(sv, std::move(v), lsm::sequence_number(o()));
        }
        ++ops;
        last = o;
    });
    if (ops > 0) {
        co_await _db.apply(std::move(wb));
        _last_applied = last;
    }
}

ss::future<std::optional<model::offset>> kv_index::lookup(bytes_view key) {
    auto holder = _gate.hold();
    auto v = co_await _db.get(
      std::string_view(reinterpret_cast<const char*>(key.data()), key.size()));
    if (!v) {
        co_return std::nullopt;
    }
    iobuf_const_parser parser(*v);
    auto x = parser.consume_be_type<uint64_t>();
    co_return model::offset(static_cast<int64_t>(x));
}

model::offset kv_index::last_applied() const { return _last_applied; }

ss::future<> kv_index::flush() {
    co_await _db.flush(
      ssx::lowres_steady_clock().now()
      + ssx::duration::from_chrono(std::chrono::seconds(5)));
}

ss::future<> kv_index::close() {
    co_await _gate.close();
    co_await _db.close();
}

ss::future<> kv_index::remove(std::filesystem::path dir) {
    if (co_await ss::file_exists(dir.native())) {
        co_await ss::recursive_remove_directory(dir);
    }
}

} // namespace storage
