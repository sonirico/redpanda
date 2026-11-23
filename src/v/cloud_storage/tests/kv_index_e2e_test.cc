/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/cloud_metadata/tests/manual_mixin.h"
#include "cluster/kv_indexer.h"
#include "cluster/partition.h"
#include "cluster/topic_properties.h"
#include "http/client.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/test/utils.h"
#include "redpanda/tests/fixture.h"
#include "storage/log.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include <boost/beast/http/status.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string_view>

using namespace std::chrono_literals;
namespace bh = boost::beast::http;

namespace {

const model::topic topic{"kv"};
const model::ntp ntp{model::kafka_namespace, topic, 0};

} // namespace

class KvIndexTieredFixture
  : public s3_imposter_fixture
  , public manual_metadata_upload_mixin
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture
  , public ::testing::Test {
public:
    KvIndexTieredFixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        test_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
          .set_value(true);
        test_local_cfg.get("kv_index_enabled").set_value(true);
        wait_for_controller_leadership().get();
    }

    http::client make_proxy_client() {
        net::base_transport::configuration transport_cfg;
        transport_cfg.server_addr = net::unresolved_address{
          "localhost", *proxy_port};
        return http::client(transport_cfg);
    }

    consumed_response lookup(http::client& client, std::string_view key) {
        return http_request(client, fmt::format("/kv/{}/{}", topic(), key));
    }

    /// Produces "key<i>" -> "val<i>": 12 records in two segments that are
    /// uploaded, then 6 that stay local. Evicts the uploaded ones from the
    /// local log once the index has read everything.
    void produce_and_evict(http::client& client) {
        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        // Local retention only trims a log whose policy includes delete.
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction
            | model::cleanup_policy_bitflags::deletion;
        props.kv_index_enabled = true;
        props.retention_local_target_bytes = tristate<size_t>(1);
        add_topic({model::kafka_namespace, topic}, 1, props).get();
        wait_for_leader(ntp).get();
        auto partition = app.partition_manager.local().get(ntp);
        auto log = partition->log();
        auto& archiver = partition->archiver().value().get();
        archiver.initialize_probe();
        ASSERT_TRUE(archiver.sync_for_tests().get());

        tests::remote_segment_generator gen(
          make_kafka_client().get(), *partition);
        auto deferred_g_close = ss::defer([&gen] { gen.stop().get(); });
        ASSERT_EQ(
          18,
          gen.num_segments(2)
            .batches_per_segment(2)
            .records_per_batch(3)
            .additional_local_segments(1)
            .produce()
            .get());
        for (int i = 0; i < 200; ++i) {
            if (lookup(client, "key17").headers.result() == bh::status::ok) {
                break;
            }
            ss::sleep(100ms).get();
        }
        ASSERT_EQ(lookup(client, "key0").body, "val0");

        ss::abort_source as;
        storage::housekeeping_config housekeeping_conf(
          model::timestamp::min(),
          1,
          log->stm_hookset()->max_removable_local_log_offset(),
          log->stm_hookset()->max_removable_local_log_offset(),
          log->stm_hookset()->max_removable_local_log_offset(),
          std::nullopt,
          std::nullopt,
          std::chrono::milliseconds{0},
          as);
        log->housekeeping(housekeeping_conf).get();
        // The storage layer only requests eviction; raft writes a snapshot
        // and truncates afterwards.
        RPTEST_REQUIRE_EVENTUALLY(10s, [partition] {
            return partition->raft_start_offset() > model::offset{0};
        });
    }

    scoped_config test_local_cfg;
};

TEST_F(KvIndexTieredFixture, EvictedKeyIs404WithoutRemoteRead) {
    auto client = make_proxy_client();
    produce_and_evict(client);

    auto res = lookup(client, "key0");

    EXPECT_EQ(res.headers.result(), bh::status::not_found);
    EXPECT_EQ(
      res.body, R"({"error":"record only available in tiered storage"})");
}

TEST_F(KvIndexTieredFixture, EvictedKeyReadsThroughWithRemoteRead) {
    auto client = make_proxy_client();
    produce_and_evict(client);
    test_local_cfg.get("kv_index_remote_read_enabled").set_value(true);

    auto res = lookup(client, "key0");

    EXPECT_EQ(res.headers.result(), bh::status::ok);
    EXPECT_EQ(res.body, "val0");
}

TEST_F(KvIndexTieredFixture, LocalKeyIsServedAfterEviction) {
    auto client = make_proxy_client();
    produce_and_evict(client);

    auto res = lookup(client, "key17");

    EXPECT_EQ(res.headers.result(), bh::status::ok);
    EXPECT_EQ(res.body, "val17");
}
