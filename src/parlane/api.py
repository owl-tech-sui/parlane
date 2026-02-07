"""Public API: pmap, pfilter, pfor, pstarmap.

These functions provide dead-simple parallel data processing.
GIL detection is automatic — threads on free-threaded Python,
processes on standard Python.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, TypeVar, overload

from parlane._backend import create_backend
from parlane._config import Config
from parlane._detection import recommended_backend
from parlane._progress import make_progress_bar, resolve_progress
from parlane._types import BackendType, Err, ErrorStrategy, Ok, ProgressType, Result

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from concurrent.futures import Future

T = TypeVar("T")
R = TypeVar("R")

_MAX_WORKERS = 32


def _resolve_workers(workers: int, backend: BackendType, n_items: int) -> int:
    """Compute optimal worker count based on backend and task count.

    - Thread backend: min(32, cpu+4, n_items) — I/O-bound benefits from
      more threads than CPU cores.
    - Process backend: min(cpu, n_items) — CPU-bound gains nothing beyond
      physical core count.
    - Never create more workers than items.
    """
    if workers > 0:
        return workers

    resolved = backend if backend != "auto" else recommended_backend()
    cpu = os.cpu_count() or 4

    if resolved == "thread":
        default = min(_MAX_WORKERS, cpu + 4)
    else:
        default = min(_MAX_WORKERS, cpu)

    return min(default, max(1, n_items))


def _compute_chunksize(n_items: int, workers: int) -> int:
    """Compute a reasonable chunksize for ProcessPoolExecutor."""
    if n_items == 0:
        return 1
    chunksize, extra = divmod(n_items, workers * 4)
    if extra:
        chunksize += 1
    return max(1, chunksize)


def _apply_error_strategy(
    fn: Callable[[T], R],
    items: Sequence[T],
    config: Config,
    backend_instance: Any,
    progress_bar: Any = None,
) -> list[Any]:
    """Execute fn over items respecting the error strategy."""
    on_error = config.on_error
    chunksize = config.chunksize or _compute_chunksize(len(items), config.workers)

    # Fast path: no progress, raise strategy — use executor.map() directly
    if progress_bar is None and on_error == "raise":
        return list(
            backend_instance.map(
                fn,
                iter(items),
                chunksize=chunksize,
                timeout=config.timeout,
            )
        )

    # submit + as_completed path for progress and/or skip/collect strategies
    from concurrent.futures import as_completed

    futures_map: dict[Future[R], int] = {}
    for i, item in enumerate(items):
        future = backend_instance.submit(fn, item)
        futures_map[future] = i

    results_with_index: list[tuple[int, Any]] = []

    for future in as_completed(futures_map):
        idx = futures_map[future]
        try:
            result = future.result(timeout=config.timeout)
            if on_error == "skip" or on_error == "raise":
                results_with_index.append((idx, result))
            else:  # collect
                results_with_index.append((idx, Ok(result)))
        except Exception as exc:
            if on_error == "raise":
                if progress_bar is not None:
                    progress_bar.close()
                raise
            elif on_error == "skip":
                pass
            else:  # collect
                results_with_index.append((idx, Err(exc)))
        finally:
            if progress_bar is not None:
                progress_bar.update(1)

    results_with_index.sort(key=lambda x: x[0])
    return [r for _, r in results_with_index]


@overload
def pmap(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int = 0,
    backend: BackendType = "auto",
    timeout: float | None = None,
    chunksize: int | None = None,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> list[R]: ...


@overload
def pmap(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int = 0,
    backend: BackendType = "auto",
    timeout: float | None = None,
    chunksize: int | None = None,
    on_error: ErrorStrategy = "collect",
    progress: ProgressType = False,
) -> list[Result[R]]: ...


def pmap(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int = 0,
    backend: BackendType = "auto",
    timeout: float | None = None,
    chunksize: int | None = None,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> list[R] | list[Result[R]]:
    """Parallel map: apply fn to each item and return results in order.

    Args:
        fn: Function to apply to each item.
        items: Iterable of input items.
        workers: Number of parallel workers (0 = auto).
        backend: "auto", "thread", or "process".
        timeout: Per-task timeout in seconds.
        chunksize: Chunk size for process backend.
        on_error: Error strategy ("raise", "skip", "collect").
        progress: Enable progress bar. True for default, string for description.
                  Requires tqdm: pip install parlane[progress]

    Returns:
        List of results in the same order as the input items.
        If on_error="collect", returns list of Ok/Err wrappers.

    Examples:
        >>> pmap(lambda x: x ** 2, [1, 2, 3])
        [1, 4, 9]
        >>> pmap(str.upper, ["hello", "world"], backend="thread")
        ['HELLO', 'WORLD']
    """
    item_list = list(items)
    if not item_list:
        return []

    resolved_workers = _resolve_workers(workers, backend, len(item_list))
    config = Config(
        workers=resolved_workers,
        backend=backend,
        timeout=timeout,
        chunksize=chunksize,
        on_error=on_error,
    )

    enabled, desc = resolve_progress(progress)
    pbar = make_progress_bar(len(item_list), desc) if enabled else None

    be = create_backend(config.backend, config.workers)
    try:
        return _apply_error_strategy(fn, item_list, config, be, progress_bar=pbar)
    finally:
        if pbar is not None:
            pbar.close()
        be.shutdown(wait=True)


def pfilter(
    fn: Callable[[T], bool],
    items: Iterable[T],
    *,
    workers: int = 0,
    backend: BackendType = "auto",
    timeout: float | None = None,
    chunksize: int | None = None,
    progress: ProgressType = False,
) -> list[T]:
    """Parallel filter: keep items where fn returns True.

    Args:
        fn: Predicate function.
        items: Iterable of input items.
        workers: Number of parallel workers (0 = auto).
        backend: "auto", "thread", or "process".
        timeout: Per-task timeout in seconds.
        chunksize: Chunk size for process backend.
        progress: Enable progress bar. True for default, string for description.

    Returns:
        List of items for which fn returned True, in original order.

    Examples:
        >>> pfilter(lambda x: x > 2, [1, 2, 3, 4, 5])
        [3, 4, 5]
    """
    item_list = list(items)
    if not item_list:
        return []

    resolved_workers = _resolve_workers(workers, backend, len(item_list))
    config = Config(
        workers=resolved_workers,
        backend=backend,
        timeout=timeout,
        chunksize=chunksize,
    )

    enabled, desc = resolve_progress(progress)
    pbar = make_progress_bar(len(item_list), desc) if enabled else None

    be = create_backend(config.backend, config.workers)
    try:
        if pbar is None:
            # Fast path: no progress bar
            csize = config.chunksize or _compute_chunksize(
                len(item_list), config.workers
            )
            mask = list(
                be.map(fn, iter(item_list), chunksize=csize, timeout=config.timeout)
            )
        else:
            # Progress path: submit + as_completed
            from concurrent.futures import as_completed

            futures_map: dict[Future[bool], int] = {}
            for i, item in enumerate(item_list):
                future = be.submit(fn, item)
                futures_map[future] = i

            mask_with_index: list[tuple[int, bool]] = []
            for future in as_completed(futures_map):
                idx = futures_map[future]
                mask_with_index.append((idx, future.result(timeout=config.timeout)))
                pbar.update(1)

            mask_with_index.sort(key=lambda x: x[0])
            mask = [v for _, v in mask_with_index]

        return [item for item, keep in zip(item_list, mask, strict=False) if keep]
    finally:
        if pbar is not None:
            pbar.close()
        be.shutdown(wait=True)


def pfor(
    fn: Callable[[T], Any],
    items: Iterable[T],
    *,
    workers: int = 0,
    backend: BackendType = "auto",
    timeout: float | None = None,
    chunksize: int | None = None,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> None:
    """Parallel for-each: apply fn to each item for side effects.

    Args:
        fn: Function to apply to each item (return value ignored).
        items: Iterable of input items.
        workers: Number of parallel workers (0 = auto).
        backend: "auto", "thread", or "process".
        timeout: Per-task timeout in seconds.
        chunksize: Chunk size for process backend.
        on_error: Error strategy ("raise", "skip", "collect").
        progress: Enable progress bar. True for default, string for description.

    Examples:
        >>> results = []
        >>> pfor(lambda x: results.append(x * 2), [1, 2, 3], backend="thread")
    """
    item_list = list(items)
    if not item_list:
        return

    resolved_workers = _resolve_workers(workers, backend, len(item_list))
    config = Config(
        workers=resolved_workers,
        backend=backend,
        timeout=timeout,
        chunksize=chunksize,
        on_error=on_error,
    )

    enabled, desc = resolve_progress(progress)
    pbar = make_progress_bar(len(item_list), desc) if enabled else None

    be = create_backend(config.backend, config.workers)
    try:
        _apply_error_strategy(fn, item_list, config, be, progress_bar=pbar)
    finally:
        if pbar is not None:
            pbar.close()
        be.shutdown(wait=True)


def _starmap_wrapper(fn: Callable[..., R], args: Any) -> R:
    """Unpack args tuple and call fn."""
    if isinstance(args, dict):
        return fn(**args)
    return fn(*args)


def pstarmap(
    fn: Callable[..., R],
    items: Iterable[Iterable[Any]],
    *,
    workers: int = 0,
    backend: BackendType = "auto",
    timeout: float | None = None,
    chunksize: int | None = None,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> list[R] | list[Result[R]]:
    """Parallel starmap: apply fn to each item with argument unpacking.

    Like pmap, but each item is unpacked as arguments to fn.

    Args:
        fn: Function to apply.
        items: Iterable of argument tuples.
        workers: Number of parallel workers (0 = auto).
        backend: "auto", "thread", or "process".
        timeout: Per-task timeout in seconds.
        chunksize: Chunk size for process backend.
        on_error: Error strategy ("raise", "skip", "collect").
        progress: Enable progress bar. True for default, string for description.

    Returns:
        List of results in order.

    Examples:
        >>> pstarmap(pow, [(2, 3), (3, 2), (10, 2)])
        [8, 9, 100]
    """
    import functools

    wrapped = functools.partial(_starmap_wrapper, fn)
    return pmap(
        wrapped,
        items,
        workers=workers,
        backend=backend,
        timeout=timeout,
        chunksize=chunksize,
        on_error=on_error,
        progress=progress,
    )
