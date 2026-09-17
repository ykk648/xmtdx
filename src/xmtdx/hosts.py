"""通达信行情服务器列表与真实可用性探活。

通达信 7709 端口有一批「半死」服务器：TCP 握手正常、``get_security_count``
也照常返回标的数量，但 K 线请求只回 2 字节的 count、不带数据。只按
``get_security_count`` 判断存活会选中这批服务器，结果是连接「成功」却永远取不到
数据。

因此这里只用**真实 K 线请求**判定可用（沪、深各一只 ETF 日线都非空），并在
``probe_hosts()`` / ``resolve_hosts()`` 之上提供磁盘缓存，避免每次启动都扫全量
候选。

典型用法::

    from xmtdx.hosts import resolve_hosts
    from xmtdx import TdxClient

    for host, port in resolve_hosts():
        try:
            with TdxClient(host, port) as client:
                ...
            break
        except Exception:
            mark_unhealthy(host)

环境变量
--------
``XMTDX_HOSTS``
    逗号分隔的服务器地址，设置后跳过缓存与探活，直接使用该列表。
``XMTDX_HOSTS_TTL``
    磁盘缓存有效期（秒），默认 21600（6 小时）。
``XMTDX_HOSTS_TIMEOUT``
    单台服务器探活超时（秒），默认 3.0。
``XMTDX_HOSTS_CACHE``
    缓存文件路径，默认 ``~/.cache/xmtdx/hosts.json``。
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
from typing import Optional, Sequence

from .models.enums import KlineCategory, Market

__all__ = [
    "DEFAULT_PORT",
    "DEFAULT_PROBE_TIMEOUT",
    "DEFAULT_TTL_SECONDS",
    "KNOWN_HOSTS",
    "PROBE_BARS",
    "candidate_hosts",
    "clear_cache",
    "last_verified_at",
    "mark_unhealthy",
    "probe_host",
    "probe_hosts",
    "refresh",
    "resolve_hosts",
]

DEFAULT_PORT = 7709
DEFAULT_TTL_SECONDS = 6 * 3600
DEFAULT_PROBE_TIMEOUT = 3.0
MAX_PROBE_WORKERS = 32
CACHE_SCHEMA = 1

# 探活要求沪、深两市 ETF 日线都非空；只看 security_count 会把半死服务器算成可用。
PROBE_BARS: tuple[tuple[int, str], ...] = (
    (Market.SH, "510300"),
    (Market.SZ, "159934"),
)
PROBE_CATEGORY = KlineCategory.DAY

CACHE_PATH = Path(
    os.environ.get("XMTDX_HOSTS_CACHE") or Path.home() / ".cache" / "xmtdx" / "hosts.json"
)

_CACHE_LOCK = Lock()
_PROBE_LOCK = Lock()
_MEMORY_CACHE: Optional[list[tuple[str, int]]] = None

# 2026-09-17 实测可用的服务器（按握手延迟升序）。列表由本模块的探活能力维护，
# 遇到大范围失效时用 ``python -m xmtdx.hosts --refresh`` 重新实测。
KNOWN_HOSTS: list[str] = [
    "122.51.120.217",  # 0.16s [injoyai]
    "119.97.185.59",  # 0.16s [injoyai]
    "101.35.121.35",  # 0.17s [injoyai]
    "121.36.225.169",  # 0.17s [injoyai]
    "124.70.199.56",  # 0.17s [injoyai]
    "111.231.113.208",  # 0.17s [injoyai]
    "150.158.160.2",  # 0.17s [injoyai]
    "124.71.187.72",  # 0.17s [injoyai]
    "123.60.84.66",  # 0.18s [injoyai]
    "124.70.133.119",  # 0.18s [injoyai]
    "124.223.163.242",  # 0.18s [injoyai]
    "118.25.98.114",  # 0.18s [injoyai]
    "111.229.247.189",  # 0.18s [injoyai]
    "124.71.187.122",  # 0.18s [injoyai]
    "122.51.232.182",  # 0.18s [injoyai]
    "123.60.70.228",  # 0.18s [injoyai]
    "123.60.73.44",  # 0.20s [injoyai]
    "180.153.18.170",  # 0.21s [local]
    "49.232.15.141",  # 0.21s [injoyai]
    "82.156.174.84",  # 0.21s [injoyai]
    "81.70.151.186",  # 0.21s [injoyai]
    "101.42.164.241",  # 0.22s [injoyai]
    "120.53.8.251",  # 0.23s [injoyai]
    "101.42.240.54",  # 0.23s [injoyai]
    "101.43.159.194",  # 0.23s [injoyai]
    "62.234.50.143",  # 0.23s [injoyai]
    "116.205.183.150",  # 0.24s [injoyai]
    "111.230.186.52",  # 0.24s [injoyai]
    "152.136.191.169",  # 0.24s [injoyai]
    "218.75.126.9",  # 0.24s [local]
    "116.205.163.254",  # 0.24s [injoyai]
    "115.238.56.198",  # 0.24s [local]
    "115.238.90.165",  # 0.24s [local]
    "117.34.114.15",  # 0.24s [local]
    "110.41.2.72",  # 0.25s [injoyai]
    "117.34.114.27",  # 0.25s [local]
    "124.71.9.153",  # 0.26s [injoyai]
    "110.41.147.114",  # 0.26s [injoyai]
    "116.205.171.132",  # 0.26s [injoyai]
    "117.34.114.17",  # 0.27s [local]
    "117.34.114.20",  # 0.28s [local]
    "117.34.114.18",  # 0.28s [local]
    "117.34.114.16",  # 0.28s [local]
    "117.34.114.14",  # 0.28s [local]
    "59.36.5.11",  # 0.28s [local]
    "81.71.32.47",  # 0.28s [injoyai]
    "58.63.254.236",  # 0.28s [local]
    "159.75.29.111",  # 0.29s [injoyai]
    "175.178.128.227",  # 0.29s [injoyai]
    "43.139.95.83",  # 0.29s [injoyai]
    "101.33.225.16",  # 0.31s [injoyai]
    "43.139.18.171",  # 0.32s [injoyai]
    "129.204.230.128",  # 0.32s [injoyai]
    "175.178.112.197",  # 0.33s [injoyai]
]


def _env_hosts() -> list[str]:
    raw = os.environ.get("XMTDX_HOSTS", "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _dedupe(pairs: Sequence[tuple[str, int]]) -> list[tuple[str, int]]:
    seen: set[tuple[str, int]] = set()
    ordered: list[tuple[str, int]] = []
    for item in pairs:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def candidate_hosts() -> list[tuple[str, int]]:
    """探活候选：``XMTDX_HOSTS`` 覆盖 > ``KNOWN_HOSTS``。"""
    forced = _env_hosts()
    if forced:
        return [(host, DEFAULT_PORT) for host in forced]
    return _dedupe((host, DEFAULT_PORT) for host in KNOWN_HOSTS)


def probe_host(
    host: str,
    port: int = DEFAULT_PORT,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    *,
    require_bars: bool = True,
) -> Optional[float]:
    """测量单台服务器完成握手 + 真实 K 线查询所需的时间（秒）。

    ``require_bars`` 为 True 时要求沪、深两市 ETF 日线都非空，以剔除
    「只答 count 不返数据」的半死服务器；为 False 时退化为握手 + ``security_count``
    的轻量探测。不可用时返回 ``None``。
    """
    from .client import TdxClient

    started = time.monotonic()
    client = None
    try:
        client = TdxClient(
            host=host, port=port, timeout=timeout, auto_reconnect=False
        )
        client.connect()
        if not client.get_security_count(Market.SH):
            return None
        if require_bars:
            for market, code in PROBE_BARS:
                if not client.get_security_bars(market, code, PROBE_CATEGORY, 0, 2):
                    return None
    except Exception:
        return None
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
    return time.monotonic() - started


def probe_hosts(
    hosts: Optional[Sequence[str]] = None,
    port: int = DEFAULT_PORT,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    *,
    require_bars: bool = True,
    max_workers: int = MAX_PROBE_WORKERS,
) -> list[tuple[str, float]]:
    """并发探活多台服务器，返回按延迟升序的 ``(host, seconds)`` 列表。

    不可用的服务器不会出现在结果里。``hosts`` 缺省为 ``KNOWN_HOSTS``。
    """
    targets = list(hosts) if hosts is not None else list(KNOWN_HOSTS)
    if not targets:
        return []

    def _one(host: str) -> Optional[float]:
        return probe_host(host, port, timeout, require_bars=require_bars)

    results: list[tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(targets))) as pool:
        for host, latency in zip(targets, pool.map(_one, targets)):
            if latency is not None:
                results.append((host, latency))
    results.sort(key=lambda item: item[1])
    return results


def _read_cache() -> Optional[dict]:
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("schema") != CACHE_SCHEMA:
        return None
    return payload


def _write_cache(hosts: Sequence[tuple[str, int]], ttl: int) -> None:
    payload = {
        "schema": CACHE_SCHEMA,
        "verified_at": time.time(),
        "ttl_seconds": int(ttl),
        "hosts": [{"host": host, "port": port} for host, port in hosts],
    }
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = CACHE_PATH.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        tmp_path.replace(CACHE_PATH)
    except OSError:
        pass


def _cached_hosts(ttl: int) -> Optional[list[tuple[str, int]]]:
    payload = _read_cache()
    if not payload:
        return None
    verified_at = payload.get("verified_at")
    if not isinstance(verified_at, (int, float)):
        return None
    if time.time() - verified_at > ttl:
        return None
    cached: list[tuple[str, int]] = []
    for item in payload.get("hosts") or []:
        if not isinstance(item, dict):
            continue
        host = item.get("host")
        port = item.get("port")
        if isinstance(host, str) and host and isinstance(port, int):
            cached.append((host, port))
    return cached or None


def last_verified_at() -> Optional[float]:
    """缓存中最近一次探活成功的时间戳（epoch 秒）。"""
    payload = _read_cache()
    value = (payload or {}).get("verified_at")
    return float(value) if isinstance(value, (int, float)) else None


def mark_unhealthy(host: str, port: int = DEFAULT_PORT) -> None:
    """运行时发现某台服务器取不到数据，立刻从缓存中剔除。

    即使剔除后列表为空也要回写：空缓存会让下次 ``resolve_hosts()`` 重新探活，
    否则其它进程会继续读到这份已经失效的列表。
    """
    global _MEMORY_CACHE

    with _CACHE_LOCK:
        payload = _read_cache() or {}
        remaining = [
            (item["host"], item["port"])
            for item in payload.get("hosts") or []
            if isinstance(item, dict)
            and item.get("port")
            and item.get("host") != host
        ]
        _write_cache(remaining, _env_int("XMTDX_HOSTS_TTL", DEFAULT_TTL_SECONDS))
        _MEMORY_CACHE = None


def clear_cache() -> None:
    """删除磁盘缓存并清空内存缓存，强制下次重新探活。"""
    global _MEMORY_CACHE

    with _CACHE_LOCK:
        try:
            CACHE_PATH.unlink()
        except OSError:
            pass
        _MEMORY_CACHE = None


def resolve_hosts(
    force: bool = False,
    ttl: Optional[int] = None,
    timeout: Optional[float] = None,
) -> list[tuple[str, int]]:
    """返回可用服务器列表（``(host, port)``，按延迟升序）。

    优先使用未过期的磁盘缓存；缓存缺失或过期时全量探活并回写。探活结果为空时
    返回空列表，由调用方决定如何报错。
    """
    global _MEMORY_CACHE

    forced = _env_hosts()
    if forced:
        return [(host, DEFAULT_PORT) for host in forced]

    ttl_value = (
        _env_int("XMTDX_HOSTS_TTL", DEFAULT_TTL_SECONDS) if ttl is None else ttl
    )
    timeout_value = (
        _env_float("XMTDX_HOSTS_TIMEOUT", DEFAULT_PROBE_TIMEOUT)
        if timeout is None
        else timeout
    )

    with _CACHE_LOCK:
        if not force:
            if _MEMORY_CACHE:
                return list(_MEMORY_CACHE)
            cached = _cached_hosts(ttl_value)
            if cached:
                _MEMORY_CACHE = cached
                return list(cached)

    # 探活较慢（全量候选约十几秒），不能占着缓存锁，否则会阻塞其它线程的
    # mark_unhealthy / 缓存读取。用独立的 _PROBE_LOCK 串行化探活即可。
    with _PROBE_LOCK:
        if not force and _MEMORY_CACHE:
            return list(_MEMORY_CACHE)
        verified = probe_hosts(timeout=timeout_value)

    resolved = [(host, DEFAULT_PORT) for host, _ in verified]
    with _CACHE_LOCK:
        _MEMORY_CACHE = resolved or None

    if resolved:
        _write_cache(resolved, ttl_value)
    return list(resolved)


def refresh() -> list[tuple[str, float]]:
    """忽略缓存重新实测全部候选，返回按延迟升序的 ``(host, seconds)``。"""
    clear_cache()
    started = time.monotonic()
    ranked = probe_hosts(timeout=_env_float("XMTDX_HOSTS_TIMEOUT", DEFAULT_PROBE_TIMEOUT))
    elapsed = time.monotonic() - started
    print(
        f"候选 {len(candidate_hosts())} 台，可用 {len(ranked)} 台，耗时 {elapsed:.0f}s"
    )
    for host, latency in ranked:
        print(f'    "{host}",  # {latency:.2f}s')
    return ranked


def main(argv: Optional[Sequence[str]] = None) -> int:
    """命令行入口：``xmtdx-hosts --refresh`` 重新实测并打印可用服务器。"""
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    if "--refresh" in args:
        return 0 if refresh() else 1
    print("用法: xmtdx-hosts --refresh")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
