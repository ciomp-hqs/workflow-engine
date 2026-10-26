"""Exception types shared by the engine, nodes and CLI."""


class WorkflowError(Exception):
    """Base class for engine errors."""


class WorkflowConfigError(WorkflowError):
    """Workflow config is invalid. ``errors`` lists every problem found."""

    def __init__(self, errors):
        self.errors = list(errors)
        super().__init__("invalid workflow config:\n  - " + "\n  - ".join(self.errors))


class NodeError(WorkflowError):
    """Retryable node failure (the default for any exception raised by a handler)."""


class NonRetryableError(NodeError):
    """Raised by a handler to fail the node immediately without further retries."""


class NodeCancelled(WorkflowError):
    """Raised inside a handler (via ctx.check_cancelled) when the run was cancelled."""


class AttemptTimeout(NodeError):
    """An attempt exceeded timeout_seconds."""


class RefError(NonRetryableError):
    """An input reference could not be resolved."""


class StaleApprovalError(WorkflowError):
    """The approval hash does not match the content currently pending approval."""

    def __init__(self, message, current_hash=None):
        super().__init__(message)
        self.current_hash = current_hash


class InvalidStateError(WorkflowError):
    """Operation not allowed in the current run/node state."""
