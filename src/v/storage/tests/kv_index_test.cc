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

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/kv_index.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"
#include "test_utils/tmp_dir.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace storage {

namespace {

iobuf to_iobuf(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

bytes_view to_bytes_view(std::string_view s) {
    return bytes_view(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

model::record_batch make_batch(
  model::offset base_offset,
  std::vector<std::pair<std::string, std::optional<std::string>>> kvs,
  model::compression compression = model::compression::none) {
    record_batch_builder builder(
      model::record_batch_type::raft_data, base_offset);
    builder.set_compression(compression);
    for (auto& [k, v] : kvs) {
        std::optional<iobuf> value;
        if (v) {
            value = to_iobuf(*v);
        }
        builder.add_raw_kv(to_iobuf(k), std::move(value));
    }
    return std::move(builder).build();
}

} // namespace

class kv_index_fixture : public seastar_test {
public:
    kv_index::options make_options() {
        return kv_index::options{.dir = _tmp_dir.get_path()};
    }

private:
    temporary_dir _tmp_dir{"kv_index_test"};
};

TEST_F_CORO(kv_index_fixture, put_and_lookup) {
    auto idx = co_await kv_index::open(make_options());
    auto batch = make_batch(model::offset(0), {{"k", "v1"}});
    co_await idx->index_batch(batch);

    auto looked_up = co_await idx->lookup(to_bytes_view("k"));
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(0));
    EXPECT_EQ(idx->last_applied(), model::offset(0));

    co_await idx->close();
}

TEST_F_CORO(kv_index_fixture, latest_wins) {
    auto idx = co_await kv_index::open(make_options());
    auto batch0 = make_batch(model::offset(0), {{"k", "v1"}});
    auto batch1 = make_batch(model::offset(1), {{"k", "v2"}});
    co_await idx->index_batch(batch0);
    co_await idx->index_batch(batch1);

    auto looked_up = co_await idx->lookup(to_bytes_view("k"));
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(1));

    co_await idx->close();
}

TEST_F_CORO(kv_index_fixture, tombstone_removes) {
    auto idx = co_await kv_index::open(make_options());
    auto batch0 = make_batch(model::offset(0), {{"k", "v1"}});
    auto batch1 = make_batch(model::offset(1), {{"k", std::nullopt}});
    co_await idx->index_batch(batch0);
    co_await idx->index_batch(batch1);

    auto looked_up = co_await idx->lookup(to_bytes_view("k"));
    EXPECT_FALSE(looked_up.has_value());

    co_await idx->close();
}

TEST_F_CORO(kv_index_fixture, null_key_skipped) {
    auto idx = co_await kv_index::open(make_options());
    record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(10));
    builder.add_raw_kv(std::nullopt, to_iobuf("unkeyed"));
    builder.add_raw_kv(to_iobuf("k"), to_iobuf("v"));
    auto batch = std::move(builder).build();
    co_await idx->index_batch(batch);

    auto looked_up = co_await idx->lookup(to_bytes_view("k"));
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(11));
    EXPECT_EQ(idx->last_applied(), model::offset(11));

    co_await idx->close();
}

TEST_F_CORO(kv_index_fixture, redelivery_ignored) {
    auto idx = co_await kv_index::open(make_options());
    auto batch = make_batch(model::offset(0), {{"k", "v1"}});
    co_await idx->index_batch(batch);
    auto last_applied_after_first = idx->last_applied();

    co_await idx->index_batch(batch);
    EXPECT_EQ(idx->last_applied(), last_applied_after_first);

    auto looked_up = co_await idx->lookup(to_bytes_view("k"));
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(0));

    co_await idx->close();
}

TEST_F_CORO(kv_index_fixture, reopen_keeps_last_applied) {
    auto opts = make_options();
    auto idx = co_await kv_index::open(opts);
    auto batch = make_batch(model::offset(0), {{"k", "v1"}});
    co_await idx->index_batch(batch);
    co_await idx->flush();
    co_await idx->close();

    auto reopened = co_await kv_index::open(opts);
    EXPECT_EQ(reopened->last_applied(), model::offset(0));

    auto looked_up = co_await reopened->lookup(to_bytes_view("k"));
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(0));

    co_await reopened->close();
}

TEST_F_CORO(kv_index_fixture, compressed_batch_indexed) {
    auto idx = co_await kv_index::open(make_options());
    auto batch = make_batch(
      model::offset(0), {{"k", "v1"}}, model::compression::zstd);
    ASSERT_TRUE(batch.compressed());
    co_await idx->index_batch(batch);

    auto looked_up = co_await idx->lookup(to_bytes_view("k"));
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(0));

    co_await idx->close();
}

} // namespace storage
