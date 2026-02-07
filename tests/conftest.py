"""Shared fixtures for parlane tests."""

from __future__ import annotations

import pytest


def square(x: int) -> int:
    """CPU-bound test function (picklable, top-level)."""
    return x * x


def double(x: int) -> int:
    return x * 2


def is_even(x: int) -> bool:
    return x % 2 == 0


def is_positive(x: int) -> bool:
    return x > 0


def add(a: int, b: int) -> int:
    return a + b


def failing_fn(x: int) -> int:
    if x == 3:
        raise ValueError(f"bad value: {x}")
    return x * 2


@pytest.fixture
def small_range() -> list[int]:
    return list(range(20))


@pytest.fixture
def medium_range() -> list[int]:
    return list(range(100))
