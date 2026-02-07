"""Error types for parlane."""

from __future__ import annotations


class ParlaneError(Exception):
    """Base exception for all parlane errors."""


class TaskError(ParlaneError):
    """Wraps an exception raised during parallel task execution.

    Attributes:
        original: The original exception that caused the failure.
        index: The index of the item that caused the error (if available).
    """

    def __init__(
        self,
        message: str,
        original: BaseException,
        *,
        index: int | None = None,
    ) -> None:
        super().__init__(message)
        self.original = original
        self.index = index

    def __repr__(self) -> str:
        idx = f", index={self.index}" if self.index is not None else ""
        return f"TaskError({self.original!r}{idx})"


class TimeoutError(ParlaneError):
    """Raised when a task exceeds its timeout."""

    def __init__(self, message: str = "Task timed out") -> None:
        super().__init__(message)


class BackendError(ParlaneError):
    """Raised when backend creation or operation fails."""
