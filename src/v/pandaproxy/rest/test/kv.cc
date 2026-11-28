// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "cluster/shard_table.h"
#include "cluster/topic_properties.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "http/client.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "utils/base64.h"

#include <seastar/core/metrics_api.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <boost/beast/http/status.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <string_view>

using namespace std::chrono_literals;

namespace ppj = pandaproxy::json;
namespace bh = boost::beast::http;

namespace {

const model::topic topic{"kv"};

struct kv_fixture : pandaproxy_test_fixture {
    kv_fixture() {
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().kv_index_enabled.set_value(true);
        }).get();
        set_client_config("retry_base_backoff_ms", 10ms);
        set_client_config("produce_batch_delay_ms", 0ms);
        wait_for_controller_leadership().get();
    }

    ~kv_fixture() {
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().kv_index_enabled.reset();
        }).get();
    }

    void add_indexed_topic(int partitions) {
        cluster::topic_properties props;
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        props.kv_index_enabled = true;
        add_topic(
          model::topic_namespace_view(model::kafka_namespace, topic),
          partitions,
          props)
          .get();
    }

    void produce(
      http::client& client,
      std::string_view key,
      std::string_view value,
      int partition) {
        auto b64 = [](std::string_view s) {
            return bytes_to_base64(
              bytes_view(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
        };
        auto json = fmt::format(
          R"({{"records":[{{"key":"{}","value":"{}","partition":{}}}]}})",
          b64(key),
          b64(value),
          partition);
        iobuf body;
        body.append(json.data(), json.size());
        auto res = http_request(
          client,
          fmt::format("/topics/{}", topic()),
          std::move(body),
          bh::verb::post,
          ppj::serialization_format::binary_v2,
          ppj::serialization_format::v2);
        BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
    }

    bool index_dir_exists() {
        const auto topic_dir = data_dir / "kafka" / std::string_view(topic());
        if (!std::filesystem::exists(topic_dir)) {
            return false;
        }
        for (const auto& ntp_dir :
             std::filesystem::directory_iterator(topic_dir)) {
            if (std::filesystem::exists(ntp_dir.path() / "kv_index")) {
                return true;
            }
        }
        return false;
    }

    consumed_response lookup(http::client& client, std::string_view key) {
        return http_request(client, fmt::format("/kv/{}/{}", topic(), key));
    }

    /// Looks `key` up until the answer is 200 with `value`. The index
    /// follows commits in the background, so the first answers may lag.
    consumed_response wait_for_value(
      http::client& client, std::string_view key, std::string_view value) {
        consumed_response res;
        for (int i = 0; i < 200; ++i) {
            res = lookup(client, key);
            if (res.headers.result() == bh::status::ok && res.body == value) {
                break;
            }
            ss::sleep(100ms).get();
        }
        BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, value);
        return res;
    }
};

} // namespace

FIXTURE_TEST(lookup_returns_latest_value, kv_fixture) {
    add_indexed_topic(1);
    auto client = make_proxy_client();
    produce(client, "k", "v1", 0);
    produce(client, "k", "v2", 0);

    auto res = wait_for_value(client, "k", "v2");

    BOOST_REQUIRE_EQUAL(res.headers.at("X-Kv-Partition"), "0");
    BOOST_REQUIRE_EQUAL(res.headers.at("X-Kv-Offset"), "1");
}

FIXTURE_TEST(lookup_of_unknown_key_is_404, kv_fixture) {
    add_indexed_topic(1);
    auto client = make_proxy_client();
    produce(client, "k", "v", 0);
    wait_for_value(client, "k", "v");

    auto res = lookup(client, "other");

    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::not_found);
}

FIXTURE_TEST(lookup_on_topic_without_index_is_404, kv_fixture) {
    add_topic(model::topic_namespace_view(model::kafka_namespace, topic)).get();
    auto client = make_proxy_client();

    auto res = lookup(client, "k");

    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::not_found);
    BOOST_REQUIRE_EQUAL(res.body, "topic not indexed");
}

FIXTURE_TEST(lookup_finds_keys_placed_by_default_partitioner, kv_fixture) {
    // Partitions that rpk (franz-go, same as the Java client's default
    // partitioner) chose for these keys on a 3 partition topic. For each of
    // them murmur2(key) % 3 differs from the partition with the sign bit
    // cleared.
    struct placement {
        std::string_view key;
        int partition;
    };
    const std::array placements{
      placement{"k0", 2},
      placement{"k8", 2},
      placement{"k9", 2},
      placement{"k12", 0},
      placement{"k13", 2},
      placement{"k15", 0},
    };
    add_indexed_topic(3);
    auto client = make_proxy_client();
    for (const auto& p : placements) {
        produce(client, p.key, p.key, p.partition);
    }

    for (const auto& p : placements) {
        auto res = wait_for_value(client, p.key, p.key);
        BOOST_REQUIRE_EQUAL(
          res.headers.at("X-Kv-Partition"), fmt::format("{}", p.partition));
    }
}

FIXTURE_TEST(lookup_served_after_restart_without_new_writes, kv_fixture) {
    add_indexed_topic(1);
    {
        auto client = make_proxy_client();
        produce(client, "k", "v", 0);
        wait_for_value(client, "k", "v");
    }

    restart(should_wipe::no);

    wait_for_controller_leadership().get();
    auto client = make_proxy_client();
    wait_for_value(client, "k", "v");
}

FIXTURE_TEST(lookup_of_partition_not_hosted_is_421, kv_fixture) {
    add_indexed_topic(1);
    auto client = make_proxy_client();

    auto res = http_request(
      client, fmt::format("/kv/{}/k?partition=1", topic()));

    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::misdirected_request);
}

FIXTURE_TEST(lookup_after_index_disabled_is_404_and_drops_index, kv_fixture) {
    add_indexed_topic(1);
    auto client = make_proxy_client();
    produce(client, "k", "v", 0);
    wait_for_value(client, "k", "v");
    BOOST_REQUIRE(index_dir_exists());
    cluster::topic_properties_update update({model::kafka_namespace, topic});
    update.properties.kv_index_enabled.op
      = cluster::incremental_update_operation::set;
    update.properties.kv_index_enabled.value = false;
    cluster::topic_properties_update_vector updates;
    updates.push_back(std::move(update));

    auto results = app.controller->get_topics_frontend()
                     .local()
                     .update_topic_properties(
                       std::move(updates), model::no_timeout)
                     .get();

    BOOST_REQUIRE_EQUAL(results.size(), 1);
    BOOST_REQUIRE_EQUAL(results[0].ec, cluster::errc::success);
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] { return !index_dir_exists(); });
    auto res = lookup(client, "k");
    BOOST_REQUIRE_EQUAL(res.headers.result(), bh::status::not_found);
    BOOST_REQUIRE_EQUAL(res.body, "topic not indexed");
}

FIXTURE_TEST(deleting_indexed_topic_removes_its_directory, kv_fixture) {
    add_indexed_topic(1);
    auto client = make_proxy_client();
    produce(client, "k", "v", 0);
    wait_for_value(client, "k", "v");
    BOOST_REQUIRE(index_dir_exists());

    delete_topic(model::topic_namespace(model::kafka_namespace, topic)).get();

    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        return !std::filesystem::exists(
          data_dir / "kafka" / std::string_view(topic()));
    });
}

FIXTURE_TEST(index_exports_next_offset_metric, kv_fixture) {
    ss::smp::invoke_on_all([] {
        config::shard_local_cfg().disable_metrics.set_value(false);
    }).get();
    add_indexed_topic(1);
    const model::ntp ntp(model::kafka_namespace, topic, model::partition_id(0));
    auto shard = app.shard_table.local().shard_for(ntp);
    BOOST_REQUIRE(shard.has_value());

    auto has_topic_metric = [] {
        const auto& values = ss::metrics::impl::get_value_map();
        auto family = values.find("cluster_kv_index_next_offset");
        if (family == values.end()) {
            return false;
        }
        return std::ranges::any_of(family->second, [](const auto& metric) {
            const auto& labels = metric.first.labels();
            auto label = labels.find(metrics::topic_label.name());
            return label != labels.end() && label->second.value() == topic();
        });
    };

    auto exported
      = ss::smp::submit_to(*shard, std::move(has_topic_metric)).get();

    BOOST_REQUIRE(exported);
}
