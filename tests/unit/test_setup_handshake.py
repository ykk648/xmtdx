"""握手策略回归测试：默认不发会被大批服务器拒绝的 0x0FDB 二次登录。

背景见 ``xmtdx/commands/setup.py``：第三条 setup 命令（type 0x0FDB）被一批云上
行情主站拒绝后，它们会把 K 线降级成「只回 2 字节 count、不带数据」，看起来像
服务器半死。这里锁定「默认两条命令、可选三条」这一约定。
"""

from __future__ import annotations

import asyncio

import pytest

from tests.unit.test_sync_transport import _frame, _ScriptedSocket
from xmtdx import AsyncTdxClient, Market, TdxClient
from xmtdx.commands.setup import (
    SETUP_CMD1,
    SETUP_CMD2,
    SETUP_CMD3,
    SETUP_COMMANDS,
    SETUP_COMMANDS_WITH_LOGIN,
)
from xmtdx.exceptions import TdxConnectionError
from xmtdx.transport.sync import TdxConnection


def test_default_handshake_skips_the_0fdb_login() -> None:
    assert SETUP_COMMANDS == (SETUP_CMD1, SETUP_CMD2)
    assert SETUP_CMD3 not in SETUP_COMMANDS
    assert SETUP_COMMANDS_WITH_LOGIN == (SETUP_CMD1, SETUP_CMD2, SETUP_CMD3)


def test_connection_sends_two_commands_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    sock = _ScriptedSocket(_frame(b"") * 2)
    monkeypatch.setattr("xmtdx.transport.sync.socket.socket", lambda *a, **k: sock)
    conn = TdxConnection("127.0.0.1", timeout=0.2)

    conn.connect()
    conn.close()

    assert sock.requests == [SETUP_CMD1, SETUP_CMD2]


def test_connection_can_opt_into_the_legacy_handshake(monkeypatch: pytest.MonkeyPatch) -> None:
    sock = _ScriptedSocket(_frame(b"") * 3)
    monkeypatch.setattr("xmtdx.transport.sync.socket.socket", lambda *a, **k: sock)
    conn = TdxConnection("127.0.0.1", timeout=0.2, setup_commands=SETUP_COMMANDS_WITH_LOGIN)

    conn.connect()
    conn.close()

    assert sock.requests == [SETUP_CMD1, SETUP_CMD2, SETUP_CMD3]


def test_sync_client_forwards_setup_commands() -> None:
    assert TdxClient("127.0.0.1")._conn._setup_commands == SETUP_COMMANDS
    assert (
        TdxClient("127.0.0.1", setup_commands=SETUP_COMMANDS_WITH_LOGIN)._conn._setup_commands
        == SETUP_COMMANDS_WITH_LOGIN
    )


def test_reconnect_keeps_configured_handshake(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[tuple[object, ...]] = []

    class _RecordingConnection:
        def __init__(self, host: str, port: int, timeout: float, setup_commands=None) -> None:
            created.append((host, port, timeout, setup_commands))
            self.is_connected = False

        def connect(self) -> None:
            self.is_connected = True

        def close(self) -> None:
            self.is_connected = False

        def execute(self, cmd: object) -> None:
            raise TdxConnectionError("boom")

    monkeypatch.setattr("xmtdx.client.TdxConnection", _RecordingConnection)
    client = TdxClient(
        "127.0.0.1", timeout=0.2, max_attempts=2, setup_commands=SETUP_COMMANDS_WITH_LOGIN
    )

    with pytest.raises(TdxConnectionError):
        client.get_security_count(Market.SH)

    assert len(created) == 2
    assert all(item[3] == SETUP_COMMANDS_WITH_LOGIN for item in created)


def test_async_client_forwards_setup_commands() -> None:
    async def main() -> None:
        default_client = AsyncTdxClient("127.0.0.1", timeout=0.2)
        assert default_client._conn._setup_commands == SETUP_COMMANDS

        legacy_client = AsyncTdxClient(
            "127.0.0.1", timeout=0.2, setup_commands=SETUP_COMMANDS_WITH_LOGIN
        )
        assert legacy_client._conn._setup_commands == SETUP_COMMANDS_WITH_LOGIN

    asyncio.run(main())
