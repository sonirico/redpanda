// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/kv_handlers.h"

#include "bytes/bytes.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "config/rest_authn_endpoint.h"
#include "kafka/client/partitioners.h"
#include "kafka/client/types.h"
#include "kafka/data/partition_proxy.h"
#include "model/batch_compression.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/httpd.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/request_auth.h"
#include "storage/kv_index.h"
#include "storage/log.h"
#include "storage/translating_reader.h"

namespace pandaproxy::rest {

namespace {

struct kv_lookup_result {
    enum class status { ok, not_hosted, not_indexed, not_found } status;
    iobuf value;
    model::partition_id partition;
    kafka::offset offset;
};

} // namespace

ss::future<proxy::server::reply_t>
get_kv(proxy::server::request_t rq, proxy::server::reply_t rp) {
    auto topic = parse::request_param<model::topic>(*rq.req, "topic_name");
    auto key = parse::request_param<ss::sstring>(*rq.req, "key");
    auto partition = parse::query_param<std::optional<model::partition_id>>(
      *rq.req, "partition");

    if (rq.authn_method != config::rest_authn_method::none) {
        auto auth_result = rq.context().authenticator.authenticate(*rq.req);
        auth_result.pass();
        auto principal = security::acl_principal{
          security::principal_type::user, rq.user.name};
        auto host = security::acl_host{rq.req->get_client_address().addr()};
        auto& groups = auth_result.get_groups();
        auto is_authorized = rq.service()
                               .authorizer()
                               .authorized(
                                 topic,
                                 security::acl_operation::read,
                                 principal,
                                 host,
                                 security::superuser_required::no,
                                 groups)
                               .is_authorized();
        if (!is_authorized) {
            rp.rep->set_status(ss::http::reply::status_type::forbidden);
            co_return std::move(rp);
        }
    }
    rq.req.reset();

    auto& tt = rq.service().topic_table();
    auto md = tt.get_topic_metadata_ref(
      model::topic_namespace_view(model::kafka_namespace, topic));
    if (!md) {
        rp.rep->set_status(
          ss::http::reply::status_type::not_found, "topic not found");
        co_return std::move(rp);
    }

    if (!partition) {
        kafka::client::record_essence re;
        re.key = iobuf::from(key);
        auto p = kafka::client::murmur2_key_partitioner()(
          re, md->get().get_assignments().size());
        if (!p) {
            rp.rep->set_status(
              ss::http::reply::status_type::bad_request, "empty key");
            co_return std::move(rp);
        }
        partition = *p;
    }

    model::ntp ntp{model::kafka_namespace, topic, *partition};
    auto shard = rq.service().shard_table().shard_for(ntp);
    kv_lookup_result res;
    if (!shard) {
        res.status = kv_lookup_result::status::not_hosted;
    } else {
        res = co_await rq.service().partition_manager().invoke_on(
          *shard,
          [ntp, key](this auto, cluster::partition_manager& pm)
            -> ss::future<kv_lookup_result> {
              auto p = pm.get(ntp);
              if (!p) {
                  co_return kv_lookup_result{
                    kv_lookup_result::status::not_hosted};
              }
              auto* idx = p->log()->get_kv_index();
              if (!idx) {
                  co_return kv_lookup_result{
                    kv_lookup_result::status::not_indexed};
              }
              auto off = co_await idx->lookup(bytes_view(
                reinterpret_cast<const uint8_t*>(key.data()), key.size()));
              if (!off) {
                  co_return kv_lookup_result{
                    kv_lookup_result::status::not_found};
              }
              auto koff = *off;
              auto proxy = kafka::make_partition_proxy(p);
              auto rdr = co_await proxy.make_reader(
                kafka::log_reader_config(koff, koff, std::nullopt));
              auto batches = co_await model::consume_reader_to_memory(
                std::move(rdr.reader), model::no_timeout);

              for (auto& batch : batches) {
                  model::record_batch decompressed;
                  const model::record_batch* target = &batch;
                  if (batch.compressed()) {
                      decompressed = co_await model::decompress_batch(batch);
                      target = &decompressed;
                  }
                  std::optional<model::record> found;
                  target->for_each_record([&](model::record r) {
                      if (
                        target->base_offset() + model::offset(r.offset_delta())
                        == model::offset(koff)) {
                          found = std::move(r);
                          return ss::stop_iteration::yes;
                      }
                      return ss::stop_iteration::no;
                  });
                  if (found) {
                      if (!found->has_key() || !(found->key() == key)) {
                          co_return kv_lookup_result{
                            kv_lookup_result::status::not_found};
                      }
                      if (!found->has_value()) {
                          co_return kv_lookup_result{
                            kv_lookup_result::status::not_found};
                      }
                      co_return kv_lookup_result{
                        .status = kv_lookup_result::status::ok,
                        .value = found->share_value(),
                        .partition = ntp.tp.partition,
                        .offset = koff};
                  }
              }
              co_return kv_lookup_result{kv_lookup_result::status::not_found};
          });
    }

    switch (res.status) {
    case kv_lookup_result::status::ok:
        rp.rep->set_status(ss::http::reply::status_type::ok);
        rp.rep->add_header("X-Kv-Partition", fmt::format("{}", res.partition));
        rp.rep->add_header("X-Kv-Offset", fmt::format("{}", res.offset));
        rp.rep->write_body(
          "octet-stream", json::as_body_writer(std::move(res.value)));
        rp.mime_type = json::serialization_format::application_octet;
        break;
    case kv_lookup_result::status::not_hosted:
        rp.rep->set_status(
          static_cast<ss::http::reply::status_type>(421),
          R"({"error":"partition not hosted on this node"})");
        break;
    case kv_lookup_result::status::not_indexed:
        rp.rep->set_status(
          ss::http::reply::status_type::not_found, "topic not indexed");
        break;
    case kv_lookup_result::status::not_found:
        rp.rep->set_status(ss::http::reply::status_type::not_found);
        break;
    }

    co_return std::move(rp);
}

} // namespace pandaproxy::rest
