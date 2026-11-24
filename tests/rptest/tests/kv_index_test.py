# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests
from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception

PANDAPROXY_PORT = 8082
TOPIC = "kv-index-test"


class KvIndexTest(RedpandaTest):
    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context,
            num_brokers=3,
            extra_rp_conf={"kv_index_enabled": True},
            pandaproxy_config=PandaproxyConfig(),
        )

    def _lookup(self, topic: str, key: str) -> requests.Response:
        last = None
        for node in self.redpanda.nodes:
            r = requests.get(
                f"http://{node.account.hostname}:{PANDAPROXY_PORT}/kv/{topic}/{key}",
                timeout=10,
            )
            if r.status_code != 421:
                return r
            last = r
        assert last is not None
        return last

    def _wait_lookup(
        self, topic: str, key: str, status: int, body: bytes | None = None
    ) -> None:
        wait_until(
            lambda: (r := self._lookup(topic, key)).status_code == status
            and (body is None or r.content == body),
            timeout_sec=60,
            backoff_sec=1,
        )

    @cluster(num_nodes=3)
    def test_lookup_latest_value(self) -> None:
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            TOPIC,
            partitions=3,
            replicas=3,
            config={
                "cleanup.policy": "compact",
                "redpanda.kv.index.enabled": "true",
            },
        )
        for i in range(100):
            rpk.produce(TOPIC, f"k{i}", f"v-{i}-1")
        for i in range(100):
            rpk.produce(TOPIC, f"k{i}", f"v-{i}-2")
        rpk.produce(TOPIC, "k5", "", tombstone=True)

        self._wait_lookup(TOPIC, "k7", 200, b"v-7-2")
        r = self._lookup(TOPIC, "k7")
        assert "X-Kv-Partition" in r.headers

        self._wait_lookup(TOPIC, "k5", 404)
        self._wait_lookup(TOPIC, "missing", 404)

        plain_topic = "kv-index-plain"
        rpk.create_topic(
            plain_topic,
            partitions=1,
            replicas=3,
            config={"cleanup.policy": "compact"},
        )
        rpk.produce(plain_topic, "k0", "v0")
        self._wait_lookup(plain_topic, "k0", 404, b"topic not indexed")

    @cluster(num_nodes=3)
    def test_lookup_after_restart_without_new_writes(self) -> None:
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            TOPIC,
            partitions=1,
            replicas=3,
            config={
                "cleanup.policy": "compact",
                "redpanda.kv.index.enabled": "true",
            },
        )
        rpk.produce(TOPIC, "k", "v")
        self._wait_lookup(TOPIC, "k", 200, b"v")

        self.redpanda.restart_nodes(self.redpanda.nodes)

        self._wait_lookup(TOPIC, "k", 200, b"v")

    def _feature_state(self, node: ClusterNode) -> str:
        features = Admin(self.redpanda).get_features(node=node)["features"]
        return next(f["state"] for f in features if f["name"] == "kv_index")

    def _set_feature(self, state: str) -> None:
        Admin(self.redpanda).put_feature("kv_index", {"state": state})
        wait_until(
            lambda: all(self._feature_state(n) == state for n in self.redpanda.nodes),
            timeout_sec=30,
            backoff_sec=1,
        )

    @cluster(num_nodes=3)
    def test_property_rejected_while_feature_inactive(self) -> None:
        """
        During an upgrade the feature is inactive until every broker runs the
        new version. The property must not be settable in that window.
        """
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            TOPIC, partitions=1, replicas=3, config={"cleanup.policy": "compact"}
        )
        self._set_feature("disabled")

        with expect_exception(RpkException, lambda e: "INVALID_CONFIG" in str(e)):
            rpk.create_topic(
                "kv-index-gated",
                partitions=1,
                replicas=3,
                config={
                    "cleanup.policy": "compact",
                    "redpanda.kv.index.enabled": "true",
                },
            )
        with expect_exception(RpkException, lambda e: "INVALID_CONFIG" in str(e)):
            rpk.alter_topic_config(TOPIC, "redpanda.kv.index.enabled", "true")

        self._set_feature("active")
        rpk.alter_topic_config(TOPIC, "redpanda.kv.index.enabled", "true")


class KvIndexTieredTest(RedpandaTest):
    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            log_segment_size=1024 * 1024,
            retention_local_strict=True,
        )
        super().__init__(
            test_context,
            num_brokers=3,
            si_settings=si_settings,
            extra_rp_conf={"kv_index_enabled": True},
            pandaproxy_config=PandaproxyConfig(),
        )

    def _lookup(self, topic: str, key: str) -> requests.Response:
        last = None
        for node in self.redpanda.nodes:
            r = requests.get(
                f"http://{node.account.hostname}:{PANDAPROXY_PORT}/kv/{topic}/{key}",
                timeout=10,
            )
            if r.status_code != 421:
                return r
            last = r
        assert last is not None
        return last

    def _wait_lookup(
        self, topic: str, key: str, status: int, body: bytes | None = None
    ) -> None:
        wait_until(
            lambda: (r := self._lookup(topic, key)).status_code == status
            and (body is None or r.content == body),
            timeout_sec=60,
            backoff_sec=1,
        )

    def _produce_until_evicted(self) -> str:
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            TOPIC,
            partitions=2,
            replicas=3,
            config={
                # Local retention only trims a log whose policy has delete.
                "cleanup.policy": "compact,delete",
                "redpanda.kv.index.enabled": "true",
                "redpanda.remote.write": "true",
                "redpanda.remote.read": "true",
                "segment.bytes": "1048576",
                "retention.local.target.bytes": "128",
            },
        )
        value = "x" * 256
        for i in range(20000):
            rpk.produce(TOPIC, f"k{i}", value)

        admin = Admin(self.redpanda)

        def local_data_evicted() -> bool:
            for p in (0, 1):
                status = admin.get_partition_cloud_storage_status(TOPIC, p)
                if not (
                    status["local_log_start_offset"] > status["cloud_log_start_offset"]
                ):
                    return False
            return True

        wait_until(local_data_evicted, timeout_sec=180, backoff_sec=2)

        return value

    @cluster(num_nodes=3)
    def test_evicted_key_is_404_by_default(self) -> None:
        value = self._produce_until_evicted()

        self._wait_lookup(
            TOPIC, "k1", 404, b'{"error":"record only available in tiered storage"}'
        )
        self._wait_lookup(TOPIC, "k19999", 200, value.encode())

    @cluster(num_nodes=3)
    def test_evicted_key_reads_through_when_enabled(self) -> None:
        value = self._produce_until_evicted()

        self.redpanda.set_cluster_config({"kv_index_remote_read_enabled": True})

        self._wait_lookup(TOPIC, "k1", 200, value.encode())
        self._wait_lookup(TOPIC, "k19999", 200, value.encode())
