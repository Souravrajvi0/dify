from unittest.mock import MagicMock

import pytest
from graphon.graph_events.node import NodeRunSucceededEvent
from graphon.node_events import NodeRunResult

from core.workflow.graphon_runtime_patches import apply_graphon_stop_execution_patches


@pytest.fixture(autouse=True)
def _reset_patch_state():
    import core.workflow.graphon_runtime_patches as patch_module

    patch_module._PATCHES_APPLIED = False
    yield
    patch_module._PATCHES_APPLIED = False


def _success_handler():
    from graphon.graph_engine.event_management.event_handlers import EventHandler

    return EventHandler.__dict__["_dispatch"].dispatcher.registry[NodeRunSucceededEvent]


def test_apply_graphon_stop_execution_patches_is_idempotent() -> None:
    apply_graphon_stop_execution_patches()
    first_handler = _success_handler()
    apply_graphon_stop_execution_patches()
    second_handler = _success_handler()

    assert first_handler is second_handler


def test_aborted_success_event_skips_downstream_scheduling() -> None:
    apply_graphon_stop_execution_patches()

    from graphon.graph_engine.event_management.event_handlers import EventHandler

    frame = MagicMock()
    handler = EventHandler(
        graph_execution=MagicMock(aborted=True),
        event_collector=MagicMock(),
        frame_registry=MagicMock(),
        container_handlers={},
    )
    handler._collect = MagicMock()
    event = NodeRunSucceededEvent(
        id="exec-1",
        node_id="node-1",
        node_type="llm",
        start_at=1.0,
        finished_at=2.0,
        node_run_result=NodeRunResult(status="succeeded", inputs={}, process_data={}, outputs={}),
    )

    _success_handler()(handler, event, frame=frame)

    frame.state_manager.finish_execution.assert_called_once_with("node-1")
    handler._collect.assert_called_once_with(frame=frame, event=event)
    frame.edge_processor.process_node_success.assert_not_called()
    frame.state_manager.enqueue_node.assert_not_called()


def test_dispatcher_skips_drain_when_aborted() -> None:
    apply_graphon_stop_execution_patches()

    from graphon.graph_engine.orchestration.dispatcher import Dispatcher

    dispatcher = Dispatcher.__new__(Dispatcher)
    dispatcher._graph_execution = MagicMock(aborted=True)
    dispatcher._process_commands = MagicMock()
    dispatcher._drain_event_queue = MagicMock()

    Dispatcher._drain_after_exit(dispatcher, paused=False)

    dispatcher._process_commands.assert_called_once_with()
    dispatcher._drain_event_queue.assert_not_called()


def test_process_commands_stops_worker_pool_when_aborted() -> None:
    apply_graphon_stop_execution_patches()

    from graphon.graph_engine.orchestration.dispatcher import Dispatcher

    dispatcher = Dispatcher.__new__(Dispatcher)
    dispatcher._graph_execution = MagicMock(aborted=True)
    dispatcher._worker_pool = MagicMock()
    dispatcher._command_processor = MagicMock()

    Dispatcher._process_commands(dispatcher, None)

    dispatcher._command_processor.process_commands.assert_called_once_with()
    dispatcher._worker_pool.stop.assert_called_once_with()
