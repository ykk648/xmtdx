"""握手命令原始字节（从 pytdx/parser/setup_commands.py 移植，已在真实服务器验证）。

连接建立后按 ``SETUP_COMMANDS`` 的顺序发送握手命令，每条都要读取并丢弃响应。

为什么默认不发 SETUP_CMD3
-------------------------
``SETUP_CMD3`` 是 type ``0x0FDB`` 的「二次登录」（带客户端指纹）。一批跑在腾讯云/
华为云上的行情主站（``KNOWN_HOSTS`` 里的大多数）会**拒绝**它：

    请求: 0c 03 1899 00 01 2000 2000 db0f ...
    响应: b1cb7400 0c 03189900 00 db0f <GBK:客户端与行情主站不匹…>
                   ^^ ctrl=0x0c 表示失败

被拒之后这些服务器不会断开，而是把 K 线请求降级成「只回 2 字节的 count、不带
数据」：

    请求 day K 线 -> ctrl=0x0c zlen=2 body=2003   (0x0320=800，只有 count)

于是现象变成 ``get_security_count`` 正常、``get_security_bars`` 永远为空，很容易
被误判成「服务器半死」。只发前两条命令时这些服务器完全正常。

实测（2026-09-17，147 台候选服务器）
------------------------------------
| 握手                        | 可用服务器 |
| 三条 setup（旧默认）         |      9    |
| 只发前两条（新默认）         |     72    |
| 只有发第三条才行             |      0    |

需要旧行为时显式传 ``SETUP_COMMANDS_WITH_LOGIN``。
"""

from typing import Final

# 从 pytdx 源码原文复制，去除空格
SETUP_CMD1: Final[bytes] = bytes.fromhex("0c0218930001030003000d0001")
SETUP_CMD2: Final[bytes] = bytes.fromhex("0c0218940001030003000d0002")
SETUP_CMD3: Final[bytes] = bytes.fromhex(
    "0c031899000120002000db0f"
    "d5d0c9ccd6a4a8af0000008f"
    "c22540130000d500c9ccbdf0"
    "d7ea00000002"
)

# 默认握手：不发会被大批服务器拒绝的 0x0FDB 二次登录
SETUP_COMMANDS: Final[tuple[bytes, ...]] = (SETUP_CMD1, SETUP_CMD2)

# 旧行为（三条命令），仅在遇到确实需要二次登录的服务器时使用
SETUP_COMMANDS_WITH_LOGIN: Final[tuple[bytes, ...]] = (SETUP_CMD1, SETUP_CMD2, SETUP_CMD3)
