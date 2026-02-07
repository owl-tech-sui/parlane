"""Tests for Pipeline lazy composition engine."""

from __future__ import annotations

import pytest

from parlane import pipeline
from parlane._pipeline import Pipeline

# -- Top-level functions for process backend pickling --


def double(x: int) -> int:
    return x * 2


def square(x: int) -> int:
    return x * x


def is_even(x: int) -> bool:
    return x % 2 == 0


def is_positive(x: int) -> bool:
    return x > 0


def negate(x: int) -> int:
    return -x


def explode(x: int) -> list[int]:
    return [x, x * 10, x * 100]


def to_empty_list(x: int) -> list[int]:
    return []


def gte_100(x: int) -> bool:
    return x >= 100


def gt_5(x: int) -> bool:
    return x > 5


def gt_10(x: int) -> bool:
    return x > 10


def gt_50(x: int) -> bool:
    return x > 50


def failing_on_three(x: int) -> int:
    if x == 3:
        raise ValueError(f"bad: {x}")
    return x * 2


class TestPipelineCreation:
    """Tests for pipeline factory and Pipeline construction."""

    def test_factory_returns_pipeline(self) -> None:
        p = pipeline([1, 2, 3])
        assert isinstance(p, Pipeline)

    def test_accepts_list(self) -> None:
        result = pipeline([1, 2, 3]).collect()
        assert result == [1, 2, 3]

    def test_accepts_range(self) -> None:
        result = pipeline(range(5)).collect()
        assert result == [0, 1, 2, 3, 4]

    def test_accepts_generator(self) -> None:
        gen = (x for x in range(3))
        result = pipeline(gen).collect()
        assert result == [0, 1, 2]

    def test_empty_input(self) -> None:
        result = pipeline([]).collect()
        assert result == []

    def test_repr(self) -> None:
        p = pipeline([1]).map(double).filter(is_even)
        r = repr(p)
        assert "Pipeline" in r
        assert "map" in r
        assert "filter" in r


class TestPipelineMap:
    """Tests for .map() intermediate operation."""

    def test_single_map(self) -> None:
        result = pipeline([1, 2, 3]).map(double).collect()
        assert result == [2, 4, 6]

    def test_chained_maps(self) -> None:
        result = pipeline([1, 2, 3]).map(double).map(square).collect()
        assert result == [4, 16, 36]

    def test_preserves_order(self) -> None:
        items = list(range(100))
        result = pipeline(items).map(double).collect()
        assert result == [x * 2 for x in items]

    def test_with_thread_backend(self) -> None:
        result = pipeline([1, 2, 3]).backend("thread").map(double).collect()
        assert result == [2, 4, 6]


class TestPipelineFilter:
    """Tests for .filter() intermediate operation."""

    def test_single_filter(self) -> None:
        result = pipeline(range(10)).filter(is_even).collect()
        assert result == [0, 2, 4, 6, 8]

    def test_filter_all_pass(self) -> None:
        result = pipeline([2, 4, 6]).filter(is_even).collect()
        assert result == [2, 4, 6]

    def test_filter_none_pass(self) -> None:
        result = pipeline([1, 3, 5]).filter(is_even).collect()
        assert result == []

    def test_preserves_order(self) -> None:
        items = list(range(50))
        result = pipeline(items).filter(is_even).collect()
        expected = [x for x in items if x % 2 == 0]
        assert result == expected


class TestPipelineFlatMap:
    """Tests for .flat_map() intermediate operation."""

    def test_basic_flat_map(self) -> None:
        result = pipeline([1, 2, 3]).flat_map(explode).collect()
        assert result == [1, 10, 100, 2, 20, 200, 3, 30, 300]

    def test_flat_map_empty_results(self) -> None:
        result = pipeline([1, 2]).flat_map(to_empty_list).collect()
        assert result == []

    def test_flat_map_then_filter(self) -> None:
        result = pipeline([1, 2]).flat_map(explode).filter(gte_100).collect()
        assert result == [100, 200]


class TestPipelineBatch:
    """Tests for .batch() intermediate operation."""

    def test_even_batches(self) -> None:
        result = pipeline(range(6)).batch(2).collect()
        assert result == [[0, 1], [2, 3], [4, 5]]

    def test_uneven_batches(self) -> None:
        result = pipeline(range(5)).batch(2).collect()
        assert result == [[0, 1], [2, 3], [4]]

    def test_batch_size_equals_items(self) -> None:
        result = pipeline([1, 2, 3]).batch(3).collect()
        assert result == [[1, 2, 3]]

    def test_batch_size_larger_than_items(self) -> None:
        result = pipeline([1, 2]).batch(10).collect()
        assert result == [[1, 2]]

    def test_invalid_batch_size(self) -> None:
        with pytest.raises(ValueError, match="batch size must be >= 1"):
            pipeline([1]).batch(0)

    def test_batch_then_map(self) -> None:
        result = pipeline(range(6)).batch(3).map(sum).collect()
        assert result == [3, 12]


class TestPipelineChaining:
    """Tests for complex chained operations."""

    def test_map_filter_map(self) -> None:
        result = pipeline(range(10)).map(double).filter(gt_10).map(negate).collect()
        expected = [-x * 2 for x in range(10) if x * 2 > 10]
        assert result == expected

    def test_filter_map_filter(self) -> None:
        result = pipeline(range(20)).filter(is_even).map(square).filter(gt_50).collect()
        expected = [x * x for x in range(20) if x % 2 == 0 and x * x > 50]
        assert result == expected

    def test_empty_after_filter(self) -> None:
        result = pipeline([1, 3, 5]).filter(is_even).map(double).collect()
        assert result == []


class TestPipelineTerminals:
    """Tests for terminal operations."""

    def test_collect(self) -> None:
        result = pipeline([3, 1, 2]).collect()
        assert result == [3, 1, 2]

    def test_reduce_sum(self) -> None:
        result = pipeline([1, 2, 3, 4]).reduce(sum)
        assert result == 10

    def test_reduce_max(self) -> None:
        result = pipeline([3, 1, 4, 1, 5]).reduce(max)
        assert result == 5

    def test_reduce_set(self) -> None:
        result = pipeline([1, 2, 2, 3, 3]).reduce(set)
        assert result == {1, 2, 3}

    def test_count(self) -> None:
        result = pipeline(range(10)).filter(is_even).count()
        assert result == 5

    def test_count_empty(self) -> None:
        result = pipeline([]).count()
        assert result == 0

    def test_first(self) -> None:
        result = pipeline([10, 20, 30]).first()
        assert result == 10

    def test_first_with_filter(self) -> None:
        result = pipeline(range(10)).filter(gt_5).first()
        assert result == 6

    def test_first_empty(self) -> None:
        result = pipeline([]).first()
        assert result is None

    def test_first_none_pass_filter(self) -> None:
        result = pipeline([1, 3, 5]).filter(is_even).first()
        assert result is None


class TestPipelineImmutability:
    """Tests that Pipeline is immutable and reusable."""

    def test_map_returns_new_pipeline(self) -> None:
        p1 = pipeline([1, 2, 3])
        p2 = p1.map(double)
        assert p1 is not p2

    def test_original_unmodified(self) -> None:
        p1 = pipeline([1, 2, 3])
        p2 = p1.map(double)
        assert p1.collect() == [1, 2, 3]
        assert p2.collect() == [2, 4, 6]

    def test_reuse_base_pipeline(self) -> None:
        base = pipeline(range(10))
        evens = base.filter(is_even).collect()
        doubled = base.map(double).collect()
        assert evens == [0, 2, 4, 6, 8]
        assert doubled == [x * 2 for x in range(10)]


class TestPipelineConfig:
    """Tests for pipeline configuration methods."""

    def test_workers_config(self) -> None:
        result = pipeline([1, 2, 3]).workers(2).map(double).collect()
        assert result == [2, 4, 6]

    def test_backend_config(self) -> None:
        result = pipeline([1, 2, 3]).backend("thread").map(double).collect()
        assert result == [2, 4, 6]

    def test_on_error_skip(self) -> None:
        result = (
            pipeline([1, 2, 3, 4, 5]).on_error("skip").map(failing_on_three).collect()
        )
        assert result == [2, 4, 8, 10]

    def test_config_chaining(self) -> None:
        result = pipeline([1, 2, 3]).workers(2).backend("thread").map(double).collect()
        assert result == [2, 4, 6]
