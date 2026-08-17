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

#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "storage/kv_index.h"
#include "storage/ntp_config.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <gtest/gtest.h>

#include <optional>
#include <string_view>
#include <utility>

namespace {

model::record_batch
make_keyed_batch(std::string_view key, std::string_view value) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    iobuf k;
    k.append(key.data(), key.size());
    iobuf v;
    v.append(value.data(), value.size());
    builder.add_raw_kv(std::move(k), std::move(v));
    return std::move(builder).build();
}

bytes_view to_bytes_view(std::string_view s) {
    return bytes_view(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

storage::ntp_config
make_compacted_kv_config(model::ntp ntp, ss::sstring base_dir) {
    storage::ntp_config cfg(std::move(ntp), std::move(base_dir));
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    overrides.kv_index_enabled = true;
    cfg.set_overrides(overrides);
    return cfg;
}

} // namespace

class kv_index_log_fixture : public log_builder_fixture {
public:
    void SetUp() override {
        config::shard_local_cfg().kv_index_enabled.set_value(true);
    }

    void TearDown() override {
        config::shard_local_cfg().kv_index_enabled.reset();
    }
};

TEST_F(kv_index_log_fixture, log_with_override_indexes_appends) {
    auto ntp = storage::log_builder_ntp();
    b.start(make_compacted_kv_config(ntp, b.get_log_config().base_dir)).get();
    ASSERT_TRUE(b.get_log()->notify_kv_index_update().get());
    ASSERT_NE(b.get_log()->get_kv_index(), nullptr);

    b.add_batch(make_keyed_batch("k", "v1")).get();
    b.add_batch(make_keyed_batch("k", "v2")).get();

    auto looked_up
      = b.get_log()->get_kv_index()->lookup(to_bytes_view("k")).get();
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(1));

    b.stop().get();
}

TEST_F(kv_index_log_fixture, no_override_no_index) {
    b.start(storage::log_builder_ntp()).get();
    EXPECT_EQ(b.get_log()->get_kv_index(), nullptr);
    b.stop().get();
}

TEST_F(kv_index_log_fixture, notify_update_toggles) {
    auto ntp = storage::log_builder_ntp();
    b.start(ntp).get();
    EXPECT_EQ(b.get_log()->get_kv_index(), nullptr);

    storage::ntp_config::default_overrides enabled;
    enabled.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    enabled.kv_index_enabled = true;
    b.update_configuration(enabled).get();

    ASSERT_TRUE(b.get_log()->notify_kv_index_update().get());
    EXPECT_NE(b.get_log()->get_kv_index(), nullptr);

    storage::ntp_config::default_overrides disabled;
    disabled.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    b.update_configuration(disabled).get();

    ASSERT_TRUE(b.get_log()->notify_kv_index_update().get());
    EXPECT_EQ(b.get_log()->get_kv_index(), nullptr);

    b.stop().get();
}

TEST_F(kv_index_log_fixture, reopen_resumes_from_last_applied) {
    auto ntp = storage::log_builder_ntp();
    auto base_dir = b.get_log_config().base_dir;
    b.start(make_compacted_kv_config(ntp, base_dir)).get();
    b.get_log()->notify_kv_index_update().get();

    b.add_batch(make_keyed_batch("k", "v1")).get();

    b.stop().get();
    b.start(make_compacted_kv_config(ntp, base_dir)).get();
    b.get_log()->notify_kv_index_update().get();

    auto looked_up
      = b.get_log()->get_kv_index()->lookup(to_bytes_view("k")).get();
    ASSERT_TRUE(looked_up.has_value());
    EXPECT_EQ(*looked_up, model::offset(0));

    b.stop().get();
}

TEST_F(kv_index_log_fixture, truncate_rebuilds) {
    auto ntp = storage::log_builder_ntp();
    b.start(make_compacted_kv_config(ntp, b.get_log_config().base_dir)).get();
    b.get_log()->notify_kv_index_update().get();

    b.add_batch(make_keyed_batch("k0", "v0")).get();
    b.add_batch(make_keyed_batch("k1", "v1")).get();
    b.add_batch(make_keyed_batch("k2", "v2")).get();

    b.truncate(model::offset(1)).get();

    auto* idx = b.get_log()->get_kv_index();
    ASSERT_NE(idx, nullptr);
    EXPECT_TRUE(idx->lookup(to_bytes_view("k0")).get().has_value());
    EXPECT_FALSE(idx->lookup(to_bytes_view("k1")).get().has_value());
    EXPECT_FALSE(idx->lookup(to_bytes_view("k2")).get().has_value());

    b.stop().get();
}
