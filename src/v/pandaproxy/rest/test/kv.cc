// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "cluster/topic_properties.h"
#include "config/configuration.h"
#include "http/client.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"
#include "test_utils/boost_fixture.h"
#include "utils/base64.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <boost/beast/http/status.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/format.h>

#include <array>
#include <chrono>
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
            return bytes_to_base64(bytes_view(
              reinterpret_cast<const uint8_t*>(s.data()), s.size()));
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
    add_topic(model::topic_namespace_view(model::kafka_namespace, topic))
      .get();
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
