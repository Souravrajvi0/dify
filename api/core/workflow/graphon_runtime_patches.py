"""Runtime patches for Graphon stop/abort behavior.

Graphon can keep scheduling downstream nodes after a user abort because the
dispatcher drains queued success events and the success handler does not check
``aborted`` before enqueueing ready nodes. Dify applies these patches at startup
until Graphon ships the fix upstream.
"""

from __future__ import annotations

import inspect
import logging
from typing import Final

logger = logging.getLogger(__name__)

_PATCHES_APPLIED: bool = False
_PATCH_MARKER: Final[str] = "__dify_stop_execution_patch__"


def _dispatch_method():
    from graphon.graph_engine.event_management.event_handlers import EventHandler

    return EventHandler.__dict__["_dispatch"]


def _patch_success_handler() -> None:
    from graphon.graph_engine.event_management.event_handlers import EventHandler
    from graphon.graph_events.node import NodeRunSucceededEvent

    dispatch_method = _dispatch_method()
    original_handle_success = dispatch_method.dispatcher.registry[NodeRunSucceededEvent]
    accepts_frame = "frame" in inspect.signature(original_handle_success).parameters

    if accepts_frame:

        def _handle_node_run_succeeded(self, event: NodeRunSucceededEvent, *, frame) -> None:
            if self._graph_execution.aborted:
                frame.state_manager.finish_execution(event.node_id)
                self._collect(frame=frame, event=event)
                return

            original_handle_success(self, event, frame=frame)

    else:

        def _handle_node_run_succeeded(self, event: NodeRunSucceededEvent) -> None:
            if self._graph_execution.aborted:
                self._state_manager.finish_execution(event.node_id)
                self._event_collector.collect(event)
                return

            original_handle_success(self, event)

    dispatch_method.register(NodeRunSucceededEvent, _handle_node_run_succeeded)
    setattr(EventHandler, _PATCH_MARKER, True)


def _patch_dispatcher() -> None:
    from graphon.graph_engine.orchestration import dispatcher as dispatcher_module

    if hasattr(dispatcher_module, "_DispatcherLifecycle"):
        lifecycle = dispatcher_module._DispatcherLifecycle
        original_process_commands = lifecycle._process_commands

        def _process_commands(self, event=None) -> None:
            original_process_commands(self, event)
            self.execution_coordinator.handle_abort_if_needed()

        lifecycle._process_commands = _process_commands

        original_drain_after_exit = lifecycle._drain_after_exit

        def _drain_after_exit(self, outcome) -> None:
            if self.execution_coordinator.aborted:
                self._process_commands()
                return

            original_drain_after_exit(self, outcome)

        lifecycle._drain_after_exit = _drain_after_exit
        return

    from graphon.graph_engine.orchestration.dispatcher import Dispatcher

    original_process_commands = Dispatcher._process_commands

    def _process_commands(self, event=None) -> None:
        original_process_commands(self, event)
        if self._graph_execution.aborted:
            self._worker_pool.stop()

    Dispatcher._process_commands = _process_commands

    original_drain_after_exit = Dispatcher._drain_after_exit

    def _drain_after_exit(self, paused: bool) -> None:
        if self._graph_execution.aborted:
            self._process_commands()
            return

        original_drain_after_exit(self, paused)

    Dispatcher._drain_after_exit = _drain_after_exit


def apply_graphon_stop_execution_patches() -> None:
    """Make user-initiated workflow stops halt downstream node scheduling."""
    global _PATCHES_APPLIED
    if _PATCHES_APPLIED:
        return

    from graphon.graph_engine.event_management.event_handlers import EventHandler

    if getattr(EventHandler, _PATCH_MARKER, False):
        _PATCHES_APPLIED = True
        return

    _patch_success_handler()
    _patch_dispatcher()

    _PATCHES_APPLIED = True
    logger.debug("Applied Graphon stop execution runtime patches")
