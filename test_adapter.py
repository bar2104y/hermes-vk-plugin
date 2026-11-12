"""Self-check for the Long Poll watchdog.

Simulates the diagnosed failure: the first poll GET hangs forever on a
half-open TCP socket (no exception, no timeout — just never returns).
Asserts that _poll_watchdog detects the stall, restarts the loop, and
messages flow again.

Run: python test_adapter.py
"""
import asyncio
import sys
import time
import types


def _install_stubs(handled):
    # ── gateway package stubs ─────────────────────────────────────────
    gw = types.ModuleType("gateway")
    gw_config = types.ModuleType("gateway.config")

    class Platform:
        VK = "vk"

        def __init__(self, name=""):
            self.name = name

    class PlatformConfig:
        extra = {}

    gw_config.Platform = Platform
    gw_config.PlatformConfig = PlatformConfig

    gw_platforms = types.ModuleType("gateway.platforms")
    gw_base = types.ModuleType("gateway.platforms.base")

    class MessageType:
        TEXT = "text"

    class MessageEvent:
        def __init__(self, **kw):
            self.__dict__.update(kw)
            self.source = None

    class SendResult:
        def __init__(self, success, message_id=None, error=None):
            self.success = success
            self.message_id = message_id
            self.error = error

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform
            self._running = False

        def _mark_connected(self):
            pass

        def _mark_disconnected(self):
            pass

        async def handle_message(self, event):
            await asyncio.sleep(0.5)  # simulate slow LLM processing
            handled.append(event)

    gw_base.BasePlatformAdapter = BasePlatformAdapter
    gw_base.MessageEvent = MessageEvent
    gw_base.MessageType = MessageType
    gw_base.SendResult = SendResult

    gw_session = types.ModuleType("gateway.session")

    class SessionSource:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    gw_session.SessionSource = SessionSource

    sys.modules.update({
        "gateway": gw,
        "gateway.config": gw_config,
        "gateway.platforms": gw_platforms,
        "gateway.platforms.base": gw_base,
        "gateway.session": gw_session,
    })

    # ── aiohttp stub ──────────────────────────────────────────────────
    state = {"hung": 1, "sent": 0}  # first poll GET hangs; update sent once

    class ClientTimeout:
        def __init__(self, total=None, **kwargs):
            pass

    class TCPConnector:
        def __init__(self, family=None):
            pass

    class ClientError(Exception):
        pass

    class FakeResp:
        def __init__(self, kind, url):
            self.kind, self.url = kind, url

        async def json(self):
            await asyncio.sleep(0)  # real I/O yields to the event loop
            if self.kind == "get":
                if state["hung"] > 0:
                    state["hung"] = 0
                    await asyncio.get_running_loop().create_future()  # never
                if state["sent"] == 0:
                    state["sent"] = 1
                    return {"ts": "2", "updates": [{
                        "type": "message_new",
                        "object": {"message": {
                            "id": 1, "from_id": 5, "peer_id": 5,
                            "text": "hi", "out": 0,
                        }},
                    }]}
                return {"ts": "3", "updates": []}
            if "getLongPollServer" in self.url:
                return {"response": {"server": "http://lp.test", "key": "k", "ts": "1"}}
            return {"error": {"error_code": 999, "error_msg": "stub"}}

    class _CME:
        def __init__(self, kind, url):
            self.kind, self.url = kind, url

        async def __aenter__(self):
            return FakeResp(self.kind, self.url)

        async def __aexit__(self, *exc):
            return False

    class ClientSession:
        def __init__(self, connector=None):
            self.closed = False

        async def close(self):
            self.closed = True

        def get(self, url, timeout=None):
            return _CME("get", url)

        def post(self, url, data=None, timeout=None):
            return _CME("post", url)

    fake_aiohttp = types.ModuleType("aiohttp")
    fake_aiohttp.ClientTimeout = ClientTimeout
    fake_aiohttp.TCPConnector = TCPConnector
    fake_aiohttp.ClientError = ClientError
    fake_aiohttp.ClientSession = ClientSession
    sys.modules["aiohttp"] = fake_aiohttp


def main():
    handled = []
    _install_stubs(handled)

    import adapter  # noqa: E402  (imports stubs installed above)
    from adapter import VKAdapter

    # Fast watchdog for the test
    adapter.POLL_STALL_CHECK_INTERVAL = 0.1
    adapter.POLL_STALL_THRESHOLD = 0.3

    async def run():
        a = VKAdapter(adapter.PlatformConfig)
        a._running = True

        ok = await a._get_long_poll_server()
        assert ok, "initial getLongPollServer failed"

        a._updates_queue = asyncio.Queue()
        a._last_poll_activity = time.monotonic()
        a._consumer_task = asyncio.create_task(a._consumer_loop())
        a._poll_task = asyncio.create_task(a._poll_loop())
        first_task = a._poll_task
        a._watchdog_task = asyncio.create_task(a._poll_watchdog())

        # Wait: first poll hangs → watchdog restarts loop → 2nd poll returns
        # the test message → consumer handles it → handle_message called
        deadline = time.monotonic() + 10
        while not handled and time.monotonic() < deadline:
            await asyncio.sleep(0.1)

        assert handled, "watchdog did not recover — no message received"
        assert a._poll_task is not first_task, "poll loop was not restarted"
        assert first_task.done() and first_task.cancelled(), "old hung loop not cancelled"

        # No unnecessary restart while a message is being handled:
        # slow LLM processing (0.5s in the stub) must not look like a stall
        recovered_task = a._poll_task
        await asyncio.sleep(1.0)
        assert a._poll_task is recovered_task, "loop restarted while processing"
        assert len(handled) == 1, f"expected 1 message, got {len(handled)}"

        event = handled[0]
        assert event.text == "hi", f"unexpected message: {event.text!r}"
        assert event.source.user_id == "5"

        # Cleanup
        a._running = False
        for t in (a._watchdog_task, a._poll_task, a._consumer_task):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await a._close_session()

        print("PASS: watchdog restarted hung poll loop, message delivered")

    asyncio.run(run())


if __name__ == "__main__":
    main()
