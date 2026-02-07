"""Configuration dataclass for parlane."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parlane._types import BackendType, ErrorStrategy


def _default_workers() -> int:
    """Determine default worker count from CPU count."""
    cpu = os.cpu_count()
    return min(cpu, 32) if cpu else 4


@dataclass(frozen=True, slots=True)
class Config:
    """Immutable configuration for parallel execution.

    Attributes:
        workers: Number of parallel workers. Defaults to CPU count (max 32).
        backend: Execution backend ("auto", "thread", or "process").
        timeout: Per-task timeout in seconds. None means no timeout.
        chunksize: Chunk size for process backend. None means auto.
        on_error: Error handling strategy.
    """

    workers: int = 0  # 0 means auto-detect
    backend: BackendType = "auto"
    timeout: float | None = None
    chunksize: int | None = None
    on_error: ErrorStrategy = "raise"

    def __post_init__(self) -> None:
        if self.workers == 0:
            object.__setattr__(self, "workers", _default_workers())
        if self.workers < 1:
            msg = f"workers must be >= 1, got {self.workers}"
            raise ValueError(msg)
        if self.timeout is not None and self.timeout <= 0:
            msg = f"timeout must be > 0, got {self.timeout}"
            raise ValueError(msg)
        if self.chunksize is not None and self.chunksize < 1:
            msg = f"chunksize must be >= 1, got {self.chunksize}"
            raise ValueError(msg)
