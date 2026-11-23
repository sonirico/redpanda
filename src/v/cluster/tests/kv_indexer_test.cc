// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "cluster/kv_indexer.h"
#include "cluster/rm_stm.h"
#include "cluster/tests/rm_stm_test_fixture.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/consensus.h"
#include "storage/log.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"

#include <memory>
#include <optional>
#include <string_view>

using namespace std::chrono_literals;

namespace {

constexpr auto tx_timeout = 30min;
constexpr std::string_view key = "k";

struct kv_indexer_fixture : rm_stm_test_fixture {
    kv_indexer_fixture() {
        config::shard_local_cfg().kv_index_enabled.set_value(true);
        storage::ntp_config::default_overrides overrides;
        overrides.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        overrides.kv_index_enabled = true;
        start_and_disable_auto_abort(overrides);
        start_indexer();
    }

    ~kv_indexer_fixture() {
        indexer->stop().get();
        config::shard_local_cfg().kv_index_enabled.reset();
    }

    void start_indexer() {
        indexer = std::make_unique<cluster::kv_indexer>(_raft.get(), _stm);
        indexer->start().get();
    }

    void restart_indexer() {
        indexer->stop().get();
        start_indexer();
    }

    /// Replicates one record for `key` and returns its Kafka offset.
    kafka::offset write(
      model::producer_identity pid,
      int32_t seq,
      bool transactional,
      std::string_view value,
      std::string_view k = key) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.set_producer_identity(pid.id, pid.epoch);
        if (transactional) {
            builder.set_transactional_type();
        }
        builder.add_raw_kv(iobuf::from(k), iobuf::from(value));
        auto batch = std::move(builder).build();
        batch.header().base_sequence = seq;
        batch.header().reset_size_checksum_metadata(batch.data());
        auto res = _stm
                     ->replicate(
                       model::batch_identity::from(batch.header()),
                       std::move(batch),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack))
                     .get();
        BOOST_REQUIRE(res.has_value());
        return res.value().last_offset;
    }

    void wait_stable_past(kafka::offset o) {
        RPTEST_REQUIRE_EVENTUALLY(10s, [this, o] {
            return _stm->last_stable_offset()
                   > _raft->log()->to_log_offset(kafka::offset_cast(o));
        });
    }

    void wait_indexed_past(kafka::offset o) {
        RPTEST_REQUIRE_EVENTUALLY(10s, [this, o] {
            return indexer->next_offset()
                   > _raft->log()->to_log_offset(kafka::offset_cast(o));
        });
    }

    std::optional<kafka::offset> lookup(std::string_view k = key) {
        return indexer
          ->lookup(
            bytes_view(reinterpret_cast<const uint8_t*>(k.data()), k.size()))
          .get();
    }

    std::unique_ptr<cluster::kv_indexer> indexer;

    model::producer_identity plain_pid{1, 0};
    model::producer_identity tx_pid{2, 0};
    model::tx_seq tx_seq{0};
};

} // namespace

FIXTURE_TEST(committed_tx_is_indexed, kv_indexer_fixture) {
    auto first = write(plain_pid, 0, false, "v1");
    wait_stable_past(first);
    BOOST_REQUIRE(
      (bool)_stm->begin_tx(tx_pid, tx_seq, tx_timeout, model::partition_id(0))
        .get());
    auto second = write(tx_pid, 0, true, "v2");

    auto op = _stm->commit_tx(tx_pid, tx_seq, 2'000ms).get();

    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    wait_indexed_past(second);
    BOOST_REQUIRE_EQUAL(lookup(), second);
}

FIXTURE_TEST(aborted_tx_keeps_committed_value, kv_indexer_fixture) {
    auto first = write(plain_pid, 0, false, "v1");
    wait_stable_past(first);
    BOOST_REQUIRE(
      (bool)_stm->begin_tx(tx_pid, tx_seq, tx_timeout, model::partition_id(0))
        .get());
    auto second = write(tx_pid, 0, true, "v2");

    auto op = _stm->abort_tx(tx_pid, tx_seq, 2'000ms).get();

    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    wait_stable_past(second);
    wait_indexed_past(second);
    BOOST_REQUIRE_EQUAL(lookup(), first);
}

FIXTURE_TEST(open_tx_keeps_committed_value, kv_indexer_fixture) {
    // Index from scratch once the transaction is already open, so that
    // catching up to the last stable offset proves a pass saw the whole log.
    indexer->stop().get();
    auto first = write(plain_pid, 0, false, "v1");
    BOOST_REQUIRE(
      (bool)_stm->begin_tx(tx_pid, tx_seq, tx_timeout, model::partition_id(0))
        .get());
    write(tx_pid, 0, true, "v2");
    BOOST_REQUIRE(
      _stm
        ->wait_no_throw(
          _raft->committed_offset(), model::timeout_clock::now() + 10s)
        .get());

    start_indexer();

    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        return indexer->next_offset() == _stm->last_stable_offset();
    });
    BOOST_REQUIRE_EQUAL(lookup(), first);
}

FIXTURE_TEST(restart_resumes_from_persisted_index, kv_indexer_fixture) {
    auto first = write(plain_pid, 0, false, "v1");
    wait_indexed_past(first);

    restart_indexer();

    BOOST_REQUIRE_EQUAL(lookup(), first);
}

FIXTURE_TEST(records_written_while_stopped_are_indexed, kv_indexer_fixture) {
    indexer->stop().get();
    auto first = write(plain_pid, 0, false, "v1");

    start_indexer();

    wait_indexed_past(first);
    BOOST_REQUIRE_EQUAL(lookup(), first);
}

FIXTURE_TEST(
  prefix_truncation_past_indexer_drops_stale_entry, kv_indexer_fixture) {
    // The log start moves past records the indexer has not read yet, as when
    // a follower installs a snapshot. The update of the key is in that gap,
    // so the entry indexed before it must not be served.
    auto first = write(plain_pid, 0, false, "v1");
    wait_indexed_past(first);
    indexer->stop().get();
    auto second = write(plain_pid, 1, false, "v2");
    auto second_log = _raft->log()->to_log_offset(kafka::offset_cast(second));
    BOOST_REQUIRE(
      _stm->wait_no_throw(second_log, model::timeout_clock::now() + 10s).get());
    _raft->write_snapshot(raft::write_snapshot_cfg(second_log, iobuf())).get();
    BOOST_REQUIRE_GT(_raft->log()->offsets().start_offset, second_log);

    auto other = write(plain_pid, 2, false, "v", "other");

    start_indexer();

    wait_indexed_past(other);
    BOOST_REQUIRE_EQUAL(lookup("other"), other);
    BOOST_REQUIRE(!lookup().has_value());
}
