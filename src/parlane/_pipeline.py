"""Lazy pipeline composition engine.

Builds a chain of operations (map, filter, flat_map, batch) and
executes them in parallel only when a terminal method is called
(collect, reduce, count, first).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from parlane._types import BackendType, ErrorStrategy

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


# -- Step definitions (immutable) --


@dataclass(frozen=True, slots=True)
class _MapStep:
    fn: Any  # Callable[[T], U]


@dataclass(frozen=True, slots=True)
class _FilterStep:
    fn: Any  # Callable[[T], bool]


@dataclass(frozen=True, slots=True)
class _FlatMapStep:
    fn: Any  # Callable[[T], Iterable[U]]


@dataclass(frozen=True, slots=True)
class _BatchStep:
    size: int


_Step = _MapStep | _FilterStep | _FlatMapStep | _BatchStep


class Pipeline(Generic[T]):
    """Lazy, immutable pipeline for parallel data processing.

    Operations are recorded but not executed until a terminal method
    (collect, reduce, count, first) is called.

    Each operation returns a new Pipeline instance — the original
    is unchanged and can be reused.
    """

    __slots__ = ("_backend", "_on_error", "_source", "_steps", "_workers")

    def __init__(
        self,
        source: Iterable[T],
        *,
        steps: tuple[_Step, ...] = (),
        workers: int = 0,
        backend: BackendType = "auto",
        on_error: ErrorStrategy = "raise",
    ) -> None:
        self._source = source
        self._steps = steps
        self._workers = workers
        self._backend = backend
        self._on_error = on_error

    def _with_step(self, step: _Step) -> Pipeline[Any]:
        """Return a new Pipeline with an additional step."""
        return Pipeline(
            self._source,
            steps=(*self._steps, step),
            workers=self._workers,
            backend=self._backend,
            on_error=self._on_error,
        )

    # -- Configuration --

    def workers(self, n: int) -> Pipeline[T]:
        """Set the number of parallel workers."""
        return Pipeline(
            self._source,
            steps=self._steps,
            workers=n,
            backend=self._backend,
            on_error=self._on_error,
        )

    def backend(self, backend: BackendType) -> Pipeline[T]:
        """Set the execution backend."""
        return Pipeline(
            self._source,
            steps=self._steps,
            workers=self._workers,
            backend=backend,
            on_error=self._on_error,
        )

    def on_error(self, strategy: ErrorStrategy) -> Pipeline[T]:
        """Set the error handling strategy."""
        return Pipeline(
            self._source,
            steps=self._steps,
            workers=self._workers,
            backend=self._backend,
            on_error=strategy,
        )

    # -- Intermediate operations (lazy) --

    def map(self, fn: Callable[[T], U]) -> Pipeline[U]:
        """Add a parallel map step."""
        return self._with_step(_MapStep(fn))

    def filter(self, fn: Callable[[T], bool]) -> Pipeline[T]:
        """Add a parallel filter step."""
        return self._with_step(_FilterStep(fn))

    def flat_map(self, fn: Callable[[T], Iterable[U]]) -> Pipeline[U]:
        """Add a parallel flat_map step (map + flatten)."""
        return self._with_step(_FlatMapStep(fn))

    def batch(self, size: int) -> Pipeline[list[T]]:
        """Group items into batches of the given size."""
        if size < 1:
            msg = f"batch size must be >= 1, got {size}"
            raise ValueError(msg)
        return self._with_step(_BatchStep(size))

    # -- Terminal operations (execute) --

    def _execute(self) -> list[Any]:
        """Execute all steps and return results."""
        from parlane.api import pfilter, pmap

        data: list[Any] = list(self._source)

        for step in self._steps:
            if not data:
                break

            if isinstance(step, _MapStep):
                data = pmap(
                    step.fn,
                    data,
                    workers=self._workers,
                    backend=self._backend,
                    on_error=self._on_error,
                )
            elif isinstance(step, _FilterStep):
                data = pfilter(
                    step.fn,
                    data,
                    workers=self._workers,
                    backend=self._backend,
                )
            elif isinstance(step, _FlatMapStep):
                mapped = pmap(
                    step.fn,
                    data,
                    workers=self._workers,
                    backend=self._backend,
                    on_error=self._on_error,
                )
                data = [item for sublist in mapped for item in sublist]
            elif isinstance(step, _BatchStep):
                data = [data[i : i + step.size] for i in range(0, len(data), step.size)]

        return data

    def collect(self) -> list[T]:
        """Execute the pipeline and return all results as a list."""
        return self._execute()

    def reduce(self, fn: Callable[[Iterable[T]], R]) -> R:
        """Execute the pipeline and reduce results with fn.

        Args:
            fn: A function that takes an iterable and returns a single value.
                Examples: sum, max, min, list, set.
        """
        return fn(self._execute())

    def count(self) -> int:
        """Execute the pipeline and return the number of results."""
        return len(self._execute())

    def first(self) -> T | None:
        """Execute the pipeline and return the first result, or None."""
        results = self._execute()
        return results[0] if results else None

    def __repr__(self) -> str:
        n_steps = len(self._steps)
        step_names = [
            type(s).__name__.lstrip("_").replace("Step", "").lower()
            for s in self._steps
        ]
        return f"Pipeline({', '.join(step_names)}, pending={n_steps} steps)"


def pipeline(source: Iterable[T]) -> Pipeline[T]:
    """Create a new lazy pipeline from an iterable.

    Args:
        source: Input data (list, range, generator, etc.)

    Returns:
        A Pipeline object that supports chained operations.

    Examples:
        >>> pipeline([1, 2, 3]).map(lambda x: x * 2).collect()
        [2, 4, 6]
        >>> pipeline(range(10)).filter(lambda x: x > 5).count()
        4
    """
    return Pipeline(source)
