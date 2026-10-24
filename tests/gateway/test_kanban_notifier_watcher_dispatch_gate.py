"""Notifier polling has an independent gateway config gate."""

import asyncio
from unittest.mock import MagicMock, patch

from gateway.config import Platform
from gateway.run import GatewayRunner


def _make_runner(with_adapter=False):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: MagicMock()} if with_adapter else {}
    runner._kanban_sub_fail_counts = {}
    return runner


def test_notifier_watcher_skips_when_notifications_disabled():
    runner = _make_runner(with_adapter=True)

    with patch(
        "hermes_cli.config.load_config",
        return_value={"kanban": {"notify_in_gateway": False}},
    ):
        with patch("hermes_cli.kanban_db.list_boards") as list_boards:
            asyncio.run(runner._kanban_notifier_watcher())

    list_boards.assert_not_called()


def test_notifier_watcher_polls_without_dispatch_ownership():
    """A profile gateway still polls its profile-owned subscriptions."""
    runner = _make_runner(with_adapter=True)
    past_gate = []
    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)
        # Stop after the initial delay + first per-interval sleep so the loop
        # body runs exactly once.
        if len(sleep_calls) >= 2:
            runner._running = False

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    import hermes_cli.kanban_db as _kb

    with patch(
        "hermes_cli.config.load_config",
        return_value={
            "kanban": {
                "dispatch_in_gateway": False,
                "notify_in_gateway": True,
            }
        },
    ):
        with patch.object(
            _kb, "list_boards",
            side_effect=lambda *a, **kw: past_gate.append(True) or [],
        ):
            with patch("asyncio.sleep", side_effect=fake_sleep):
                with patch("asyncio.to_thread", side_effect=fake_to_thread):
                    asyncio.run(runner._kanban_notifier_watcher())

    assert past_gate, (
        "gateways without the dispatch lock must still poll owned subscriptions"
    )


def test_dispatcher_claims_nothing_while_owner_draining():
    """A draining owner refuses every claim, so a spawned worker would only burn the card's failures."""
    runner = _make_runner()
    runner._draining = True
    dispatcher = MagicMock()

    async def stop_after_first_sleep(_interval):
        runner._running = False

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def no_sleep(_delay):
        return None

    runner._sleep_between_ticks = stop_after_first_sleep
    with patch.object(runner, "_kanban_dispatcher_boot", return_value=(None, MagicMock(), {})), \
            patch("gateway.kanban_watchers._resolve_dispatcher_settings", return_value=MagicMock(interval=1.0)), \
            patch("gateway.kanban_watchers._KanbanDispatcher", return_value=dispatcher), \
            patch("gateway.kanban_watchers._to_thread_process_service", fake_to_thread), \
            patch("gateway.kanban_watchers.asyncio.sleep", new=no_sleep):
        asyncio.run(runner._kanban_dispatcher_watcher())

    dispatcher.tick_once.assert_not_called()
