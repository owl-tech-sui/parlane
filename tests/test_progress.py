"""Tests for progress bar integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from parlane import pfilter, pfor, pipeline, pmap, pstarmap
from parlane._progress import make_progress_bar, resolve_progress

# -- resolve_progress --


class TestResolveProgress:
    def test_false(self) -> None:
        assert resolve_progress(False) == (False, None)

    def test_true(self) -> None:
        assert resolve_progress(True) == (True, None)

    def test_string(self) -> None:
        assert resolve_progress("Processing") == (True, "Processing")


# -- make_progress_bar --


class TestMakeProgressBar:
    def test_creates_bar_when_tqdm_available(self) -> None:
        bar = make_progress_bar(10, "test")
        bar.close()
        assert bar.total == 10

    def test_creates_bar_without_desc(self) -> None:
        bar = make_progress_bar(5, None)
        bar.close()
        assert bar.total == 5

    def test_import_error_without_tqdm(self) -> None:
        with (
            patch.dict("sys.modules", {"tqdm": None, "tqdm.auto": None}),
            pytest.raises(ImportError, match="pip install parlane\\[progress\\]"),
        ):
            make_progress_bar(10, None)


# -- Helper functions (top-level for pickling) --


def double(x: int) -> int:
    return x * 2


def is_even(x: int) -> bool:
    return x % 2 == 0


def add(a: int, b: int) -> int:
    return a + b


def failing_fn(x: int) -> int:
    if x == 3:
        raise ValueError(f"bad value: {x}")
    return x * 2


# -- pmap with progress --


class TestPmapProgress:
    def test_progress_true(self) -> None:
        result = pmap(double, [1, 2, 3], backend="thread", progress=True)
        assert result == [2, 4, 6]

    def test_progress_string(self) -> None:
        result = pmap(double, [1, 2, 3], backend="thread", progress="Mapping")
        assert result == [2, 4, 6]

    def test_progress_false(self) -> None:
        result = pmap(double, [1, 2, 3], backend="thread", progress=False)
        assert result == [2, 4, 6]

    def test_progress_preserves_order(self) -> None:
        items = list(range(50))
        result = pmap(double, items, backend="thread", progress=True)
        assert result == [x * 2 for x in items]

    def test_progress_with_error_skip(self) -> None:
        result = pmap(
            failing_fn, [1, 2, 3, 4], backend="thread", on_error="skip", progress=True
        )
        assert result == [2, 4, 8]

    def test_progress_with_error_collect(self) -> None:
        from parlane import Err, Ok

        result = pmap(
            failing_fn,
            [1, 2, 3, 4],
            backend="thread",
            on_error="collect",
            progress=True,
        )
        assert len(result) == 4
        assert isinstance(result[0], Ok)
        assert isinstance(result[2], Err)

    def test_progress_with_error_raise(self) -> None:
        with pytest.raises(ValueError, match="bad value: 3"):
            pmap(
                failing_fn,
                [1, 2, 3, 4],
                backend="thread",
                on_error="raise",
                progress=True,
            )

    def test_progress_empty_input(self) -> None:
        result = pmap(double, [], backend="thread", progress=True)
        assert result == []

    @patch("parlane.api.make_progress_bar")
    def test_progress_bar_updated(self, mock_make: MagicMock) -> None:
        mock_bar = MagicMock()
        mock_make.return_value = mock_bar

        pmap(double, [1, 2, 3], backend="thread", progress=True)

        assert mock_bar.update.call_count == 3
        mock_bar.close.assert_called_once()


# -- pfilter with progress --


class TestPfilterProgress:
    def test_progress_true(self) -> None:
        result = pfilter(is_even, range(10), backend="thread", progress=True)
        assert result == [0, 2, 4, 6, 8]

    def test_progress_string(self) -> None:
        result = pfilter(is_even, range(10), backend="thread", progress="Filtering")
        assert result == [0, 2, 4, 6, 8]

    @patch("parlane.api.make_progress_bar")
    def test_progress_bar_updated(self, mock_make: MagicMock) -> None:
        mock_bar = MagicMock()
        mock_make.return_value = mock_bar

        pfilter(is_even, range(6), backend="thread", progress=True)

        assert mock_bar.update.call_count == 6
        mock_bar.close.assert_called_once()


# -- pfor with progress --


class TestPforProgress:
    def test_progress_true(self) -> None:
        results: list[int] = []

        def append_double(x: int) -> None:
            results.append(x * 2)

        pfor(append_double, [1, 2, 3], backend="thread", workers=1, progress=True)
        assert sorted(results) == [2, 4, 6]

    @patch("parlane.api.make_progress_bar")
    def test_progress_bar_updated(self, mock_make: MagicMock) -> None:
        mock_bar = MagicMock()
        mock_make.return_value = mock_bar

        pfor(double, [1, 2, 3], backend="thread", progress=True)

        assert mock_bar.update.call_count == 3
        mock_bar.close.assert_called_once()


# -- pstarmap with progress --


class TestPstarmapProgress:
    def test_progress_true(self) -> None:
        result = pstarmap(add, [(1, 2), (3, 4)], backend="thread", progress=True)
        assert result == [3, 7]


# -- Pipeline with progress --


class TestPipelineProgress:
    def test_progress_method(self) -> None:
        result = pipeline([1, 2, 3]).progress("Test").map(double).collect()
        assert result == [2, 4, 6]

    def test_progress_true(self) -> None:
        result = pipeline([1, 2, 3]).progress().map(double).collect()
        assert result == [2, 4, 6]

    def test_progress_immutable(self) -> None:
        p1 = pipeline([1, 2, 3])
        p2 = p1.progress("Test")
        assert p1 is not p2
        assert p1._progress is False
        assert p2._progress == "Test"

    def test_progress_preserved_through_chaining(self) -> None:
        p = pipeline([1, 2, 3]).progress("X").map(double).filter(is_even)
        assert p._progress == "X"
