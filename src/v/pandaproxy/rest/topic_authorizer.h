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
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "pandaproxy/rest/proxy.h"
#include "security/acl.h"

namespace pandaproxy::rest {

/// Authenticates the request and answers whether its principal may perform
/// an operation on a topic. Must be built while `rq.req` is alive; the
/// authentication is only valid for requests made with
/// `rq.authn_method != none`.
class topic_authorizer {
public:
    topic_authorizer(
      proxy::server::request_t& rq, security::acl_operation operation);

    bool authorized(const model::topic& topic) const;

private:
    security::authorizer& _authorizer;
    security::acl_operation _operation;
    security::acl_principal _principal;
    security::acl_host _host;
    chunked_vector<security::acl_principal> _groups;
};

} // namespace pandaproxy::rest
