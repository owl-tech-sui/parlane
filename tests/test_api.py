"""Tests for parlane core API: pmap, pfilter, pfor, pstarmap."""

from __future__ import annotations

import pytest

from parlane import Err, Ok, pfilter, pfor, pmap, pstarmap
from parlane.api import _starmap_wrapper
from tests.conftest import add, double, failing_fn, is_even, is_positive, square


class TestPmap:
    """Tests for pmap function."""

    def test_basic_map(self, small_range: list[int]) -> None:
        result = pmap(square, small_range, backend="thread")
        expected = [x * x for x in small_range]
        assert result == expected

    def test_empty_input(self) -> None:
        result = pmap(square, [], backend="thread")
        assert result == []

    def test_single_item(self) -> None:
        result = pmap(square, [42], backend="thread")
        assert result == [1764]

    def test_preserves_order(self, medium_range: list[int]) -> None:
        result = pmap(double, medium_range, backend="thread")
        expected = [x * 2 for x in medium_range]
        assert result == expected

    def test_thread_backend(self, small_range: list[int]) -> None:
        result = pmap(square, small_range, backend="thread")
        assert result == [x * x for x in small_range]

    def test_process_backend(self, small_range: list[int]) -> None:
        result = pmap(square, small_range, backend="process")
        assert result == [x * x for x in small_range]

    def test_custom_workers(self) -> None:
        result = pmap(square, range(10), workers=2, backend="thread")
        assert result == [x * x for x in range(10)]

    def test_error_raise_strategy(self) -> None:
        with pytest.raises(ValueError, match="bad value: 3"):
            pmap(failing_fn, [1, 2, 3, 4], backend="thread", on_error="raise")

    def test_error_skip_strategy(self) -> None:
        result = pmap(failing_fn, [1, 2, 3, 4], backend="thread", on_error="skip")
        assert result == [2, 4, 8]

    def test_error_collect_strategy(self) -> None:
        result = pmap(failing_fn, [1, 2, 3, 4], backend="thread", on_error="collect")
        assert len(result) == 4
        assert isinstance(result[0], Ok)
        assert result[0].unwrap() == 2
        assert isinstance(result[1], Ok)
        assert result[1].unwrap() == 4
        assert isinstance(result[2], Err)
        assert isinstance(result[2].exception, ValueError)
        assert isinstance(result[3], Ok)
        assert result[3].unwrap() == 8

    def test_accepts_generator(self) -> None:
        gen = (x for x in range(5))
        result = pmap(square, gen, backend="thread")
        assert result == [0, 1, 4, 9, 16]

    def test_large_input(self) -> None:
        items = list(range(1000))
        result = pmap(double, items, backend="thread", workers=8)
        assert result == [x * 2 for x in items]


class TestPfilter:
    """Tests for pfilter function."""

    def test_basic_filter(self) -> None:
        result = pfilter(is_even, range(10), backend="thread")
        assert result == [0, 2, 4, 6, 8]

    def test_empty_input(self) -> None:
        result = pfilter(is_even, [], backend="thread")
        assert result == []

    def test_all_pass(self) -> None:
        result = pfilter(is_positive, [1, 2, 3], backend="thread")
        assert result == [1, 2, 3]

    def test_none_pass(self) -> None:
        result = pfilter(is_positive, [-1, -2, -3], backend="thread")
        assert result == []

    def test_preserves_order(self) -> None:
        items = list(range(50))
        result = pfilter(is_even, items, backend="thread")
        expected = [x for x in items if x % 2 == 0]
        assert result == expected

    def test_process_backend(self) -> None:
        result = pfilter(is_even, range(10), backend="process")
        assert result == [0, 2, 4, 6, 8]


class TestPfor:
    """Tests for pfor function."""

    def test_basic_for(self) -> None:
        results: list[int] = []

        def append_double(x: int) -> None:
            results.append(x * 2)

        pfor(append_double, [1, 2, 3], backend="thread", workers=1)
        assert sorted(results) == [2, 4, 6]

    def test_empty_input(self) -> None:
        pfor(square, [], backend="thread")

    def test_returns_none(self) -> None:
        result = pfor(square, [1, 2, 3], backend="thread")
        assert result is None

    def test_error_raise(self) -> None:
        with pytest.raises(ValueError, match="bad value: 3"):
            pfor(failing_fn, [1, 2, 3, 4], backend="thread", on_error="raise")

    def test_error_skip(self) -> None:
        pfor(failing_fn, [1, 2, 3, 4], backend="thread", on_error="skip")


class TestPstarmap:
    """Tests for pstarmap function."""

    def test_basic_starmap(self) -> None:
        result = pstarmap(add, [(1, 2), (3, 4), (5, 6)], backend="thread")
        assert result == [3, 7, 11]

    def test_empty_input(self) -> None:
        result = pstarmap(add, [], backend="thread")
        assert result == []

    def test_preserves_order(self) -> None:
        items = [(i, i + 1) for i in range(50)]
        result = pstarmap(add, items, backend="thread")
        expected = [a + b for a, b in items]
        assert result == expected

    def test_process_backend(self) -> None:
        result = pstarmap(pow, [(2, 3), (3, 2), (10, 2)], backend="process")
        assert result == [8, 9, 100]

    def test_error_collect(self) -> None:
        def div(a: int, b: int) -> float:
            return a / b

        result = pstarmap(
            div, [(10, 2), (10, 0), (6, 3)], backend="thread", on_error="collect"
        )
        assert len(result) == 3
        assert isinstance(result[0], Ok)
        assert result[0].unwrap() == 5.0
        assert isinstance(result[1], Err)
        assert isinstance(result[2], Ok)
        assert result[2].unwrap() == 2.0

    def test_dict_kwargs_unpacking(self) -> None:
        def greet(name: str, greeting: str = "Hello") -> str:
            return f"{greeting}, {name}"

        result = pstarmap(
            greet,
            [{"name": "Alice", "greeting": "Hi"}, {"name": "Bob"}],
            backend="thread",
        )
        assert result == ["Hi, Alice", "Hello, Bob"]


class TestStarmapWrapper:
    """Tests for _starmap_wrapper internal."""

    def test_tuple_unpacking(self) -> None:
        assert _starmap_wrapper(add, (2, 3)) == 5

    def test_dict_unpacking(self) -> None:
        def kw(a: int, b: int) -> int:
            return a - b

        assert _starmap_wrapper(kw, {"a": 10, "b": 3}) == 7
