// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/topic_authorizer.h"

#include "security/authorizer.h"
#include "security/request_auth.h"

namespace pandaproxy::rest {

topic_authorizer::topic_authorizer(
  proxy::server::request_t& rq, security::acl_operation operation)
  : _authorizer(rq.service().authorizer())
  , _operation(operation)
  , _principal(security::principal_type::user, rq.user.name)
  , _host(rq.req->get_client_address().addr()) {
    auto auth_result = rq.context().authenticator.authenticate(*rq.req);
    auth_result.pass();
    _groups = auth_result.get_groups().copy();
}

bool topic_authorizer::authorized(const model::topic& topic) const {
    return _authorizer
      .authorized(
        topic,
        _operation,
        _principal,
        _host,
        security::superuser_required::no,
        _groups)
      .is_authorized();
}

} // namespace pandaproxy::rest
