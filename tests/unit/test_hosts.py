"""服务器列表、真实 K 线探活与磁盘缓存测试。"""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from xmtdx import hosts


class _HostCacheCase(unittest.TestCase):
    """把缓存重定向到临时目录，避免污染用户缓存。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.cache_path = Path(self._tmp.name) / "hosts.json"
        patcher = patch.object(hosts, "CACHE_PATH", self.cache_path)
        patcher.start()
        self.addCleanup(patcher.stop)
        memory = patch.object(hosts, "_MEMORY_CACHE", None)
        memory.start()
        self.addCleanup(memory.stop)
        self.addCleanup(self._tmp.cleanup)


class CacheTests(_HostCacheCase):
    def test_write_then_read_respects_ttl(self):
        hosts._write_cache([("10.0.0.1", 7709), ("10.0.0.2", 7709)], ttl=3600)

        self.assertEqual(
            hosts._cached_hosts(3600),
            [("10.0.0.1", 7709), ("10.0.0.2", 7709)],
        )
        self.assertIsNone(hosts._cached_hosts(-1))

    def test_schema_mismatch_is_ignored(self):
        self.cache_path.write_text('{"schema": 999, "hosts": []}', encoding="utf-8")

        self.assertIsNone(hosts._read_cache())

    def test_mark_unhealthy_removes_host_and_invalidates_memory(self):
        hosts._write_cache([("10.0.0.1", 7709), ("10.0.0.2", 7709)], ttl=3600)

        hosts.mark_unhealthy("10.0.0.1")

        self.assertEqual(hosts._cached_hosts(3600), [("10.0.0.2", 7709)])

    def test_mark_unhealthy_last_host_empties_cache_so_next_call_reprobes(self):
        hosts._write_cache([("10.0.0.1", 7709)], ttl=3600)

        hosts.mark_unhealthy("10.0.0.1")

        # 空列表写回后 _cached_hosts 返回 None，强制下次重新探活
        self.assertIsNone(hosts._cached_hosts(3600))

    def test_clear_cache_removes_file(self):
        hosts._write_cache([("10.0.0.1", 7709)], ttl=3600)

        hosts.clear_cache()

        self.assertFalse(self.cache_path.exists())

    def test_corrupt_cache_file_is_tolerated(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text("not json", encoding="utf-8")

        self.assertIsNone(hosts._cached_hosts(3600))


class ResolveHostsTests(_HostCacheCase):
    def test_fresh_cache_short_circuits_probing(self):
        hosts._write_cache([("10.0.0.1", 7709)], ttl=3600)

        with patch.object(hosts, "probe_hosts") as probe:
            resolved = hosts.resolve_hosts()

        self.assertEqual(resolved, [("10.0.0.1", 7709)])
        probe.assert_not_called()

    def test_stale_cache_triggers_probe_and_persists(self):
        # 回填一个 10 小时前写入、ttl 只有 1 小时的过期缓存
        stale = {
            "schema": hosts.CACHE_SCHEMA,
            "verified_at": time.time() - 10 * 3600,
            "ttl_seconds": 3600,
            "hosts": [{"host": "10.0.0.1", "port": 7709}],
        }
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(stale), encoding="utf-8")

        with patch.object(hosts, "probe_hosts", return_value=[("10.0.0.9", 0.1)]) as probe:
            resolved = hosts.resolve_hosts()

        probe.assert_called_once()
        self.assertEqual(resolved, [("10.0.0.9", 7709)])
        self.assertEqual(hosts._cached_hosts(3600), [("10.0.0.9", 7709)])

    def test_force_ignores_cache(self):
        hosts._write_cache([("10.0.0.1", 7709)], ttl=3600)

        with patch.object(hosts, "probe_hosts", return_value=[("10.0.0.9", 0.1)]):
            resolved = hosts.resolve_hosts(force=True)

        self.assertEqual(resolved, [("10.0.0.9", 7709)])

    def test_empty_probe_result_returns_empty_and_leaves_no_cache(self):
        with patch.object(hosts, "probe_hosts", return_value=[]):
            resolved = hosts.resolve_hosts()

        self.assertEqual(resolved, [])
        self.assertFalse(self.cache_path.exists())

    def test_env_override_skips_cache_and_probe(self):
        with patch.dict("os.environ", {"XMTDX_HOSTS": "1.2.3.4, 5.6.7.8"}), \
                patch.object(hosts, "probe_hosts") as probe:
            resolved = hosts.resolve_hosts()

        self.assertEqual(resolved, [("1.2.3.4", 7709), ("5.6.7.8", 7709)])
        probe.assert_not_called()

    def test_candidate_hosts_dedupes_and_follows_known_hosts(self):
        candidates = hosts.candidate_hosts()

        self.assertEqual(len(candidates), len(hosts.KNOWN_HOSTS))
        self.assertEqual(
            [host for host, _ in candidates],
            list(hosts.KNOWN_HOSTS),
        )

    def test_probe_hosts_sorts_by_latency_and_drops_failures(self):
        def fake_probe(host, port, timeout, **kwargs):
            return {"slow": 1.0, "fast": 0.1}.get(host)

        with patch.object(hosts, "probe_host", side_effect=fake_probe):
            ranked = hosts.probe_hosts(["dead", "slow", "fast"])

        self.assertEqual(ranked, [("fast", 0.1), ("slow", 1.0)])


class CliTests(_HostCacheCase):
    def test_refresh_probes_and_returns_ranked_hosts(self):
        with patch.object(hosts, "probe_hosts", return_value=[("10.0.0.9", 0.1)]):
            ranked = hosts.refresh()

        self.assertEqual(ranked, [("10.0.0.9", 0.1)])

    def test_main_without_refresh_prints_usage(self):
        self.assertEqual(hosts.main([]), 2)
        self.assertEqual(hosts.main(["--refresh"]), 0)


if __name__ == "__main__":
    unittest.main()
