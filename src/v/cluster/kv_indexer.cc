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

#include "cluster/kv_indexer.h"

#include "cluster/logger.h"
#include "cluster/rm_stm.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "metrics/prometheus_sanitize.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "ssx/future-util.h"
#include "storage/kv_index.h"
#include "storage/log.h"
#include "storage/types.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>
#include <chrono>
#include <utility>

namespace cluster {

namespace {

constexpr auto lso_poll_interval = std::chrono::milliseconds(100);

std::filesystem::path index_dir(const storage::ntp_config& cfg) {
    return std::filesystem::path(cfg.work_directory()) / "kv_index";
}

struct index_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (!is_aborted(b)) {
            co_await index.index_batch(b, translator);
        }
        co_return ss::stop_iteration::no;
    }
    void end_of_stream() {}

    bool is_aborted(const model::record_batch& b) const {
        if (!b.header().attrs.is_transactional()) {
            return false;
        }
        model::producer_identity pid{
          b.header().producer_id, b.header().producer_epoch};
        return std::ranges::any_of(aborted, [&](const tx::tx_range& r) {
            return r.pid == pid && r.first <= b.base_offset()
                   && b.base_offset() <= r.last;
        });
    }

    storage::kv_index& index;
    const storage::offset_translator_state& translator;
    const chunked_vector<tx::tx_range>& aborted;
};

} // namespace

kv_indexer::kv_indexer(raft::consensus* raft, ss::shared_ptr<rm_stm> rm_stm)
  : _raft(raft)
  , _rm_stm(std::move(rm_stm)) {}

kv_indexer::~kv_indexer() = default;

ss::future<> kv_indexer::start() {
    auto dir = index_dir(_raft->log_config());
    _index = co_await storage::kv_index::open({.dir = dir});
    _next = model::next_offset(_index->last_applied());
    if (
      _index->last_applied() > _raft->log()->offsets().dirty_offset
      || has_gap()) {
        // Either the log lost a suffix the index had already seen, e.g. an
        // unclean shutdown with write caching, or see has_gap().
        co_await reset_index();
    }
    vlog(
      clusterlog.info,
      "[{}] kv index opened at {}, resuming from {}",
      _raft->ntp(),
      dir,
      _next);
    setup_metrics();
    ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
}

void kv_indexer::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    const auto& ntp = _raft->ntp();
    const std::vector<sm::label_instance> labels = {
      metrics::namespace_label(ntp.ns()),
      metrics::topic_label(ntp.tp.topic()),
      metrics::partition_label(ntp.tp.partition()),
    };
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:kv_index"),
      {sm::make_gauge(
         "next_offset",
         [this] { return _next; },
         sm::description(
           "Log offset the next indexing pass starts from. The distance to "
           "the last stable offset of the partition is the indexing lag."),
         labels),
       sm::make_counter(
         "rebuilds",
         [this] { return _rebuilds; },
         sm::description(
           "Number of times the index was dropped and rebuilt from the log."),
         labels)});
}

ss::future<> kv_indexer::stop() {
    _as.request_abort();
    co_await _gate.close();
    if (!_index) {
        co_return;
    }
    try {
        co_await _index->flush();
    } catch (...) {
        auto e = std::current_exception();
        vlog(
          clusterlog.warn, "[{}] kv index flush failed: {}", _raft->ntp(), e);
    }
    co_await _index->close();
}

ss::future<std::optional<kafka::offset>> kv_indexer::lookup(bytes_view key) {
    if (!_index) {
        return ss::make_ready_future<std::optional<kafka::offset>>(
          std::nullopt);
    }
    return _index->lookup(key);
}

bool kv_indexer::has_gap() const {
    return _index->last_applied() >= model::offset(0)
           && _next < _raft->log()->offsets().start_offset;
}

ss::future<> kv_indexer::reset_index() {
    auto dir = index_dir(_raft->log_config());
    vlog(clusterlog.info, "[{}] rebuilding kv index at {}", _raft->ntp(), dir);
    _caught_up = false;
    ++_rebuilds;
    if (auto old = std::exchange(_index, nullptr)) {
        co_await old->close();
    }
    co_await storage::kv_index::remove(dir);
    _index = co_await storage::kv_index::open({.dir = dir});
    _next = model::next_offset(_index->last_applied());
}

ss::future<> kv_indexer::remove_index(const storage::ntp_config& cfg) {
    return storage::kv_index::remove(index_dir(cfg));
}

ss::future<> kv_indexer::run_loop() {
    while (!_as.abort_requested()) {
        auto committed = _raft->committed_offset();
        std::exception_ptr err;
        try {
            if (_rm_stm) {
                // The last stable offset is derived from applied state, so
                // let the stm catch up with what raft already committed.
                co_await _rm_stm->wait_no_throw(
                  committed, model::no_timeout, _as);
            }
            co_await index_committed();
            if (!_caught_up) {
                // The last stable offset is not known yet, e.g. right after
                // a restart, and no commit has to follow to signal it.
                co_await ss::sleep_abortable(lso_poll_interval, _as)
                  .handle_exception([](const std::exception_ptr&) {});
                continue;
            }
            co_await _raft->commit_index_updated().wait(_as, [this, committed] {
                return _raft->committed_offset() > committed;
            });
        } catch (...) {
            err = std::current_exception();
        }
        if (!err) {
            continue;
        }
        if (ssx::is_shutdown_exception(err)) {
            break;
        }
        vlog(
          clusterlog.warn,
          "[{}] kv index pass failed, retrying: {}",
          _raft->ntp(),
          err);
        co_await ss::sleep_abortable(std::chrono::seconds(1), _as)
          .handle_exception([](const std::exception_ptr&) {});
    }
}

ss::future<> kv_indexer::index_committed() {
    auto lso = _rm_stm ? _rm_stm->last_stable_offset()
                       : model::next_offset(_raft->last_visible_index());
    if (lso == model::invalid_lso) {
        co_return;
    }
    if (!_index || has_gap()) {
        co_await reset_index();
    }
    auto log = _raft->log();
    auto start = std::max(_next, log->offsets().start_offset);
    auto last = model::prev_offset(lso);
    if (start > last) {
        _caught_up = true;
        co_return;
    }
    chunked_vector<tx::tx_range> aborted;
    if (_rm_stm) {
        aborted = co_await _rm_stm->aborted_transactions(start, last);
    }
    auto rdr = co_await log->make_reader(
      storage::local_log_reader_config(start, last, _as));
    co_await std::move(rdr).consume(
      index_consumer{*_index, *log->get_offset_translator_state(), aborted},
      model::no_timeout);
    _next = model::next_offset(last);
    _caught_up = true;
}

} // namespace cluster
