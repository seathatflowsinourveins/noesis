"""Shared pytest fixtures. Nothing here is autouse: existing tests are unaffected."""

from __future__ import annotations

import ipaddress
import os
import re
import socket
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

import pytest

# A live Alpaca key id: "AK" followed by 16 or more upper-case alphanumerics.
_LIVE_KEY_PATTERN = re.compile(r"AK[A-Z0-9]{16,}")
_LOOPBACK_NAMES = frozenset({"localhost", "localhost.", "ip6-localhost", "ip6-loopback"})


class NonLoopbackConnectBlockedError(ConnectionError):
    """Raised instead of opening a socket to a non-loopback address."""


@dataclass
class LiveBrokerAccessGuard:
    """What ``no_live_broker_access`` yields: its findings, for tests that assert on them."""

    blocked_connects: list[str] = field(default_factory=list)

    @staticmethod
    def live_key_env_names(env: Mapping[str, str] | None = None) -> list[str]:
        """Names (never values) of environment variables holding a live-looking Alpaca key."""
        source = os.environ if env is None else env
        return sorted(
            name for name, value in source.items() if _LIVE_KEY_PATTERN.match(str(value).strip())
        )

    @staticmethod
    def is_loopback(address: Any) -> bool:
        """True for loopback INET/INET6 addresses and for non-network (e.g. UNIX) sockets."""
        if not isinstance(address, tuple) or not address:
            return True
        host = address[0]
        if isinstance(host, (bytes, bytearray)):
            host = host.decode("ascii", "replace")
        text = str(host).strip().lower()
        if text in _LOOPBACK_NAMES:
            return True
        try:
            return ipaddress.ip_address(text.split("%", 1)[0]).is_loopback
        except ValueError:
            return False

    def refuse(self, address: Any) -> NonLoopbackConnectBlockedError:
        self.blocked_connects.append(repr(address))
        return NonLoopbackConnectBlockedError(
            f"no_live_broker_access blocked a non-loopback socket connect to {address!r}"
        )


@pytest.fixture
def no_live_broker_access(monkeypatch: pytest.MonkeyPatch) -> Iterator[LiveBrokerAccessGuard]:
    """Fail the test on a live-looking Alpaca key in the environment or any off-host connect.

    * Setup and teardown both fail the test when an environment value starts with ``AK``
      followed by 16+ upper-case alphanumerics (only variable names are reported).
    * ``socket.socket.connect`` / ``connect_ex`` raise ``NonLoopbackConnectBlockedError`` for any
      non-loopback address, before a packet is sent. An attempt that the code under test swallows
      still fails the test at teardown. A test that provokes a block on purpose acknowledges it
      with ``guard.blocked_connects.clear()``.

    Opt in per test or per module (``pytestmark = pytest.mark.usefixtures(...)``); not autouse.
    It is function-scoped, so a Hypothesis ``@given`` test that uses it needs
    ``suppress_health_check=[HealthCheck.function_scoped_fixture]``. The guard sits on the socket
    object, which covers ``requests``/``urllib3``, ``ssl`` and asyncio's selector event loop; it
    does not see asyncio's Windows proactor loop, which connects below the socket object.
    """
    guard = LiveBrokerAccessGuard()
    if names := guard.live_key_env_names():
        pytest.fail(f"live-looking Alpaca key in environment variable(s): {', '.join(names)}")

    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def guarded_connect(self: socket.socket, address: Any, /) -> None:
        if not guard.is_loopback(address):
            raise guard.refuse(address)
        real_connect(self, address)

    def guarded_connect_ex(self: socket.socket, address: Any, /) -> int:
        if not guard.is_loopback(address):
            raise guard.refuse(address)
        return real_connect_ex(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    yield guard
    if names := guard.live_key_env_names():
        pytest.fail(f"live-looking Alpaca key in environment variable(s): {', '.join(names)}")
    if guard.blocked_connects:
        pytest.fail(
            "non-loopback socket connect attempted: " + ", ".join(guard.blocked_connects),
        )
