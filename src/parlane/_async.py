"""Async parallel API: apmap, apfilter, apfor.

Uses ``asyncio.Semaphore`` for concurrency limiting — no executor needed.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, TypeVar, overload

from parlane._progress import make_progress_bar, resolve_progress
from parlane._types import Err, ErrorStrategy, Ok, ProgressType, Result

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

T = TypeVar("T")
R = TypeVar("R")

_MAX_ASYNC_WORKERS = 32


def _resolve_async_workers(workers: int, n_items: int) -> int:
    """Compute concurrency limit for async tasks."""
    if workers > 0:
        return workers
    return min(_MAX_ASYNC_WORKERS, max(1, n_items))


async def _apply_async_error_strategy(
    fn: Callable[[Any], Any],
    items: Sequence[Any],
    concurrency: int,
    on_error: ErrorStrategy,
    progress_bar: Any,
) -> list[Any]:
    """Run async fn over items with semaphore-based concurrency control."""
    sem = asyncio.Semaphore(concurrency)

    async def _limited(index: int, item: Any) -> tuple[int, Any, Exception | None]:
        async with sem:
            try:
                result = await fn(item)
                return (index, result, None)
            except Exception as exc:
                return (index, None, exc)
            finally:
                if progress_bar is not None:
                    progress_bar.update(1)

    raw = await asyncio.gather(*[_limited(i, x) for i, x in enumerate(items)])
    raw_sorted = sorted(raw, key=lambda x: x[0])

    output: list[Any] = []
    for _idx, value, exc in raw_sorted:
        if exc is not None:
            if on_error == "raise":
                raise exc
            elif on_error == "skip":
                continue
            else:  # collect
                output.append(Err(exc))
        else:
            if on_error == "collect":
                output.append(Ok(value))
            else:
                output.append(value)
    return output


@overload
async def apmap(
    fn: Callable[[T], Any],
    items: Any,
    *,
    workers: int = 0,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> list[R]: ...


@overload
async def apmap(
    fn: Callable[[T], Any],
    items: Any,
    *,
    workers: int = 0,
    on_error: ErrorStrategy = "collect",
    progress: ProgressType = False,
) -> list[Result[R]]: ...


async def apmap(
    fn: Callable[[T], Any],
    items: Any,
    *,
    workers: int = 0,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> list[Any]:
    """Async parallel map: apply an async fn to each item.

    Uses ``asyncio.Semaphore`` to limit concurrency.

    Args:
        fn: Async function to apply to each item.
        items: Iterable of input items.
        workers: Max concurrent tasks (0 = auto, capped at 32).
        on_error: Error strategy ("raise", "skip", "collect").
        progress: Enable progress bar. True for default, string for description.

    Returns:
        List of results in input order.

    Examples:
        >>> import asyncio
        >>> async def double(x): return x * 2
        >>> asyncio.run(apmap(double, [1, 2, 3]))
        [2, 4, 6]
    """
    item_list = list(items)
    if not item_list:
        return []

    concurrency = _resolve_async_workers(workers, len(item_list))
    enabled, desc = resolve_progress(progress)
    pbar = make_progress_bar(len(item_list), desc) if enabled else None

    try:
        return await _apply_async_error_strategy(
            fn, item_list, concurrency, on_error, pbar
        )
    finally:
        if pbar is not None:
            pbar.close()


async def apfilter(
    fn: Callable[[T], Any],
    items: Any,
    *,
    workers: int = 0,
    progress: ProgressType = False,
) -> list[T]:
    """Async parallel filter: keep items where async fn returns True.

    Args:
        fn: Async predicate function.
        items: Iterable of input items.
        workers: Max concurrent tasks (0 = auto).
        progress: Enable progress bar.

    Returns:
        List of items for which fn returned True, in original order.

    Examples:
        >>> import asyncio
        >>> async def is_even(x): return x % 2 == 0
        >>> asyncio.run(apfilter(is_even, range(6)))
        [0, 2, 4]
    """
    item_list = list(items)
    if not item_list:
        return []

    concurrency = _resolve_async_workers(workers, len(item_list))
    enabled, desc = resolve_progress(progress)
    pbar = make_progress_bar(len(item_list), desc) if enabled else None

    sem = asyncio.Semaphore(concurrency)

    async def _check(index: int, item: T) -> tuple[int, T, bool]:
        async with sem:
            try:
                keep = await fn(item)
                return (index, item, bool(keep))
            finally:
                if pbar is not None:
                    pbar.update(1)

    try:
        raw = await asyncio.gather(*[_check(i, x) for i, x in enumerate(item_list)])
        raw_sorted = sorted(raw, key=lambda x: x[0])
        return [item for _, item, keep in raw_sorted if keep]
    finally:
        if pbar is not None:
            pbar.close()


async def apfor(
    fn: Callable[[T], Any],
    items: Any,
    *,
    workers: int = 0,
    on_error: ErrorStrategy = "raise",
    progress: ProgressType = False,
) -> None:
    """Async parallel for-each: apply async fn for side effects.

    Args:
        fn: Async function to apply (return value ignored).
        items: Iterable of input items.
        workers: Max concurrent tasks (0 = auto).
        on_error: Error strategy ("raise", "skip", "collect").
        progress: Enable progress bar.

    Examples:
        >>> import asyncio
        >>> results = []
        >>> async def append(x): results.append(x)
        >>> asyncio.run(apfor(append, [1, 2, 3]))
    """
    item_list = list(items)
    if not item_list:
        return

    concurrency = _resolve_async_workers(workers, len(item_list))
    enabled, desc = resolve_progress(progress)
    pbar = make_progress_bar(len(item_list), desc) if enabled else None

    try:
        await _apply_async_error_strategy(
            fn, item_list, concurrency, on_error, pbar
        )
    finally:
        if pbar is not None:
            pbar.close()
