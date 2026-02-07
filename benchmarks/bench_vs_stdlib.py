"""Benchmark: parlane vs concurrent.futures vs sequential.

Run:
    python benchmarks/bench_vs_stdlib.py
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from parlane import pmap


def bench(label: str, fn: object, runs: int = 3) -> tuple[float, object]:
    """Run fn multiple times and return the best time."""
    times = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = fn()  # type: ignore[operator]
        times.append(time.perf_counter() - start)
    best = min(times)
    print(f"  {label:45s} {best:.3f}s")
    return best, result


def separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# -- Task functions (module-level for pickling) --


def io_task(x: int) -> int:
    time.sleep(0.05)
    return x


def cpu_task(x: int) -> float:
    total = 0.0
    for i in range(1, 50_000):
        total += math.sin(i * 0.001 + x) * math.cos(i * 0.002)
    return total


if __name__ == "__main__":
    cpu_count = os.cpu_count() or 4
    print(f"CPU cores: {cpu_count}")
    print(f"Python multiprocessing start method: {mp.get_start_method()}")

    # --------------------------------------------------------
    # 1. I/O-bound
    # --------------------------------------------------------
    separator("1. I/O-bound (100 tasks x 50ms sleep)")
    items_io = list(range(100))

    t_seq, _ = bench("Sequential", lambda: [io_task(x) for x in items_io])
    t_parlane, _ = bench(
        "parlane pmap", lambda: pmap(io_task, items_io, backend="thread")
    )

    def _manual_thread() -> list[int]:
        with ThreadPoolExecutor() as ex:
            return list(ex.map(io_task, items_io))

    t_manual, _ = bench("concurrent.futures ThreadPool", _manual_thread)

    print()
    print(f"  parlane:    {t_seq / t_parlane:.1f}x faster than sequential")
    print(f"  c.futures:  {t_seq / t_manual:.1f}x faster than sequential")
    print(f"  overhead:   {(t_parlane - t_manual) * 1000:+.0f}ms")

    # --------------------------------------------------------
    # 2. CPU-bound
    # --------------------------------------------------------
    separator("2. CPU-bound (200 tasks x heavy math)")
    items_cpu = list(range(200))

    t_seq, _ = bench("Sequential", lambda: [cpu_task(x) for x in items_cpu])
    t_parlane, _ = bench("parlane pmap", lambda: pmap(cpu_task, items_cpu))

    def _manual_process() -> list[float]:
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(mp_context=ctx) as ex:
            return list(ex.map(cpu_task, items_cpu))

    t_manual, _ = bench("concurrent.futures ProcessPool", _manual_process)

    print()
    print(f"  parlane:    {t_seq / t_parlane:.1f}x faster than sequential")
    print(f"  c.futures:  {t_seq / t_manual:.1f}x faster than sequential")
    print(f"  overhead:   {(t_parlane - t_manual) * 1000:+.0f}ms")
