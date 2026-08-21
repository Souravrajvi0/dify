from dify_app import DifyApp


def init_app(app: DifyApp):
    from core.workflow.graphon_runtime_patches import apply_graphon_stop_execution_patches

    apply_graphon_stop_execution_patches()
    from events import event_handlers  # noqa: F401
