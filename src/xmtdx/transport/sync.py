"""同步 TCP 连接（基于 socket）。"""

import socket
import time
from collections.abc import Sequence
from types import TracebackType
from typing import TYPE_CHECKING, TypeVar

from ..codec.frame import HEADER_SIZE, decompress_body, parse_header
from ..commands.setup import SETUP_COMMANDS
from ..exceptions import TdxConnectionError, TdxError
from .capture import CapturedResponse

if TYPE_CHECKING:
    from ..commands.base import BaseCommand

T = TypeVar("T")

_DEFAULT_HOST = "180.153.18.170"
_DEFAULT_PORT = 7709
_DEFAULT_TIMEOUT = 15.0

# 已知可用的通达信行情服务器（按实测握手延迟升序，2026-09-17 实测 54/59 可用）
# 由 tdx_hosts 探活脚本维护，失效前不必频繁改动
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


def ping_host(
    host: str,
    port: int = _DEFAULT_PORT,
    timeout: float = 5.0,
) -> float | None:
    """测量服务器完成完整握手和一次业务查询所需的时间（秒）。

    返回延迟（秒），连接失败时返回 None。
    """
    from ..commands.security_count import GetSecurityCountCmd
    from ..models.enums import Market

    t0 = time.monotonic()
    try:
        with TdxConnection(host, port, timeout) as conn:
            if conn.execute(GetSecurityCountCmd(Market.SH)) <= 0:
                return None
        return time.monotonic() - t0
    except (OSError, TdxError):
        return None


def ping_all(
    hosts: list[str] = KNOWN_HOSTS,
    port: int = _DEFAULT_PORT,
    timeout: float = 5.0,
) -> list[tuple[str, float]]:
    """并发测量多台服务器延迟，返回按延迟排序的 (host, latency_seconds) 列表。

    不可达的服务器不包含在结果中。
    """
    import concurrent.futures

    results: list[tuple[str, float]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as pool:
        futures = {pool.submit(ping_host, h, port, timeout): h for h in hosts}
        for fut in concurrent.futures.as_completed(futures):
            host = futures[fut]
            latency = fut.result()
            if latency is not None:
                results.append((host, latency))
    results.sort(key=lambda t: t[1])
    return results


def _recv_exact_sock(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise TdxConnectionError("连接被服务器关闭")
        buf.extend(chunk)
    return bytes(buf)


class TdxConnection:
    """同步通达信 TCP 连接。

    使用示例::

        with TdxConnection("180.153.18.170") as conn:
            result = conn.execute(SomeCommand(...))
    """

    def __init__(
        self,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        timeout: float = _DEFAULT_TIMEOUT,
        setup_commands: Sequence[bytes] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        # 默认只发前两条命令，见 commands/setup.py 里关于 0x0FDB 的说明
        self._setup_commands: tuple[bytes, ...] = (
            tuple(setup_commands) if setup_commands is not None else SETUP_COMMANDS
        )
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        """建立 TCP 连接并完成握手（发送3条 setup 命令）。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect((self.host, self.port))
        except OSError as e:
            sock.close()
            raise TdxConnectionError(f"无法连接 {self.host}:{self.port}: {e}") from e
        self._sock = sock
        try:
            self._send_setup()
        except TdxError:
            try:
                sock.close()
            except OSError:
                pass
            self._sock = None
            raise
        except OSError as error:
            try:
                sock.close()
            except OSError:
                pass
            self._sock = None
            raise TdxConnectionError(f"握手失败 {self.host}:{self.port}: {error}") from error

    def close(self) -> None:
        """关闭连接。"""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    @property
    def is_connected(self) -> bool:
        return self._sock is not None

    def execute(self, cmd: "BaseCommand[T]") -> T:
        """执行一条命令：发送请求，接收并解压响应，返回解析结果。"""
        return self.capture(cmd).result

    def capture(self, cmd: "BaseCommand[T]") -> CapturedResponse[T]:
        """执行命令并保留请求、帧头、原始响应及解压结果。"""
        if self._sock is None:
            raise TdxConnectionError("未连接，请先调用 connect()")
        request = cmd.build_request()
        try:
            self._sock.sendall(request)
            header_buf = self._recv_exact(HEADER_SIZE)
            header = parse_header(header_buf)
            raw_body = self._recv_exact(header.zipsize)
            body = decompress_body(header, raw_body)
            result = cmd.parse_response(body)
        except OSError as e:
            self.close()
            raise TdxConnectionError(f"通信错误: {e}") from e
        except TdxError:
            self.close()
            raise
        if not cmd.reusable_connection:
            self.close()
        return CapturedResponse(request, header, raw_body, body, result)

    # ------------------------------------------------------------------ #
    # context manager
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "TdxConnection":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #

    def _send_setup(self) -> None:
        """按序发送握手命令并丢弃响应（默认两条，见 commands/setup.py）。"""
        assert self._sock is not None
        for cmd_bytes in self._setup_commands:
            self._sock.sendall(cmd_bytes)
            # 读取并丢弃握手响应
            hdr_buf = self._recv_exact(HEADER_SIZE)
            hdr = parse_header(hdr_buf)
            raw_body = self._recv_exact(hdr.zipsize)
            decompress_body(hdr, raw_body)

    def _recv_exact(self, n: int) -> bytes:
        """循环 recv 直到读满 n 字节。"""
        assert self._sock is not None
        return _recv_exact_sock(self._sock, n)
