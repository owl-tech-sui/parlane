"""Backend protocol and implementations.

Provides ThreadBackend and ProcessBackend, wrapping
concurrent.futures executors with a unified interface.
"""

from __future__ import annotations

import multiprocessing as mp
import sys
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from parlane._detection import recommended_backend
from parlane._errors import BackendError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from parlane._types import BackendType

T = TypeVar("T")
R = TypeVar("R")


class Backend(Protocol):
    """Protocol for parallel execution backends."""

    def map(
        self,
        fn: Callable[..., R],
        items: Iterator[Any],
        *,
        chunksize: int = 1,
        timeout: float | None = None,
    ) -> Iterator[R]:
        """Apply fn to each item in parallel, preserving order."""
        ...

    def submit(self, fn: Callable[..., R], *args: Any, **kwargs: Any) -> Future[R]:
        """Submit a single task for execution."""
        ...

    def shutdown(self, *, wait: bool = True) -> None:
        """Shut down the backend, releasing resources."""
        ...

    def __enter__(self) -> Backend: ...

    def __exit__(self, *exc: Any) -> None: ...


class ThreadBackend:
    """Backend using ThreadPoolExecutor."""

    def __init__(self, workers: int) -> None:
        self._executor = ThreadPoolExecutor(max_workers=workers)

    def map(
        self,
        fn: Callable[..., R],
        items: Iterator[Any],
        *,
        chunksize: int = 1,
        timeout: float | None = None,
    ) -> Iterator[R]:
        return self._executor.map(fn, items, timeout=timeout, chunksize=chunksize)

    def submit(self, fn: Callable[..., R], *args: Any, **kwargs: Any) -> Future[R]:
        return self._executor.submit(fn, *args, **kwargs)

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def __enter__(self) -> ThreadBackend:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()


def _get_mp_context() -> mp.context.BaseContext | None:
    """Get a safe multiprocessing context.

    On Unix/macOS, use "fork" to avoid the __main__ guard requirement
    that "spawn" imposes. On Windows, return None (use default spawn).
    """
    if sys.platform == "win32":
        return None
    return mp.get_context("fork")


class ProcessBackend:
    """Backend using ProcessPoolExecutor."""

    def __init__(self, workers: int) -> None:
        self._executor = ProcessPoolExecutor(
            max_workers=workers,
            mp_context=_get_mp_context(),
        )

    def map(
        self,
        fn: Callable[..., R],
        items: Iterator[Any],
        *,
        chunksize: int = 1,
        timeout: float | None = None,
    ) -> Iterator[R]:
        return self._executor.map(fn, items, timeout=timeout, chunksize=chunksize)

    def submit(self, fn: Callable[..., R], *args: Any, **kwargs: Any) -> Future[R]:
        return self._executor.submit(fn, *args, **kwargs)

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def __enter__(self) -> ProcessBackend:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()


def create_backend(
    backend_type: BackendType,
    workers: int,
) -> ThreadBackend | ProcessBackend:
    """Create a backend instance based on type.

    Args:
        backend_type: "auto", "thread", or "process".
        workers: Number of parallel workers.

    Returns:
        A ThreadBackend or ProcessBackend instance.

    Raises:
        BackendError: If the backend type is invalid.
    """
    if backend_type == "auto":
        backend_type = recommended_backend()  # type: ignore[assignment]

    if backend_type == "thread":
        return ThreadBackend(workers)
    elif backend_type == "process":
        return ProcessBackend(workers)
    else:
        msg = f"Unknown backend type: {backend_type!r}"
        raise BackendError(msg)
