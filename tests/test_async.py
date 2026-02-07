"""Tests for async API: apmap, apfilter, apfor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from parlane import Err, Ok, apfilter, apfor, apmap

# -- Async helper functions --


async def async_double(x: int) -> int:
    return x * 2


async def async_square(x: int) -> int:
    return x * x


async def async_is_even(x: int) -> bool:
    return x % 2 == 0


async def async_failing(x: int) -> int:
    if x == 3:
        raise ValueError(f"bad value: {x}")
    return x * 2


# -- apmap --


class TestApmap:
    @pytest.mark.asyncio
    async def test_basic_map(self) -> None:
        result = await apmap(async_double, [1, 2, 3])
        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_empty_input(self) -> None:
        result = await apmap(async_double, [])
        assert result == []

    @pytest.mark.asyncio
    async def test_single_item(self) -> None:
        result = await apmap(async_square, [5])
        assert result == [25]

    @pytest.mark.asyncio
    async def test_preserves_order(self) -> None:
        import asyncio

        async def slow_double(x: int) -> int:
            await asyncio.sleep(0.01 * (10 - x))
            return x * 2

        items = list(range(10))
        result = await apmap(slow_double, items)
        assert result == [x * 2 for x in items]

    @pytest.mark.asyncio
    async def test_custom_workers(self) -> None:
        result = await apmap(async_double, range(10), workers=2)
        assert result == [x * 2 for x in range(10)]

    @pytest.mark.asyncio
    async def test_error_raise(self) -> None:
        with pytest.raises(ValueError, match="bad value: 3"):
            await apmap(async_failing, [1, 2, 3, 4], on_error="raise")

    @pytest.mark.asyncio
    async def test_error_skip(self) -> None:
        result = await apmap(async_failing, [1, 2, 3, 4], on_error="skip")
        assert result == [2, 4, 8]

    @pytest.mark.asyncio
    async def test_error_collect(self) -> None:
        result = await apmap(async_failing, [1, 2, 3, 4], on_error="collect")
        assert len(result) == 4
        assert isinstance(result[0], Ok)
        assert result[0].unwrap() == 2
        assert isinstance(result[1], Ok)
        assert result[1].unwrap() == 4
        assert isinstance(result[2], Err)
        assert isinstance(result[2].exception, ValueError)
        assert isinstance(result[3], Ok)
        assert result[3].unwrap() == 8

    @pytest.mark.asyncio
    async def test_large_input(self) -> None:
        result = await apmap(async_double, range(200), workers=20)
        assert result == [x * 2 for x in range(200)]

    @pytest.mark.asyncio
    async def test_progress(self) -> None:
        result = await apmap(async_double, [1, 2, 3], progress=True)
        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_progress_string(self) -> None:
        result = await apmap(async_double, [1, 2, 3], progress="Mapping")
        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    @patch("parlane._async.make_progress_bar")
    async def test_progress_bar_updated(self, mock_make: MagicMock) -> None:
        mock_bar = MagicMock()
        mock_make.return_value = mock_bar

        await apmap(async_double, [1, 2, 3], progress=True)

        assert mock_bar.update.call_count == 3
        mock_bar.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_accepts_generator(self) -> None:
        gen = (x for x in range(5))
        result = await apmap(async_double, gen)
        assert result == [0, 2, 4, 6, 8]


# -- apfilter --


class TestApfilter:
    @pytest.mark.asyncio
    async def test_basic_filter(self) -> None:
        result = await apfilter(async_is_even, range(10))
        assert result == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_empty_input(self) -> None:
        result = await apfilter(async_is_even, [])
        assert result == []

    @pytest.mark.asyncio
    async def test_all_pass(self) -> None:
        result = await apfilter(async_is_even, [2, 4, 6])
        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_none_pass(self) -> None:
        result = await apfilter(async_is_even, [1, 3, 5])
        assert result == []

    @pytest.mark.asyncio
    async def test_preserves_order(self) -> None:
        items = list(range(50))
        result = await apfilter(async_is_even, items)
        assert result == [x for x in items if x % 2 == 0]

    @pytest.mark.asyncio
    async def test_custom_workers(self) -> None:
        result = await apfilter(async_is_even, range(10), workers=3)
        assert result == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_progress(self) -> None:
        result = await apfilter(async_is_even, range(10), progress=True)
        assert result == [0, 2, 4, 6, 8]


# -- apfor --


class TestApfor:
    @pytest.mark.asyncio
    async def test_basic_for(self) -> None:
        results: list[int] = []

        async def append_double(x: int) -> None:
            results.append(x * 2)

        await apfor(append_double, [1, 2, 3])
        assert sorted(results) == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_empty_input(self) -> None:
        await apfor(async_double, [])

    @pytest.mark.asyncio
    async def test_returns_none(self) -> None:
        result = await apfor(async_double, [1, 2, 3])
        assert result is None

    @pytest.mark.asyncio
    async def test_error_raise(self) -> None:
        with pytest.raises(ValueError, match="bad value: 3"):
            await apfor(async_failing, [1, 2, 3, 4], on_error="raise")

    @pytest.mark.asyncio
    async def test_error_skip(self) -> None:
        await apfor(async_failing, [1, 2, 3, 4], on_error="skip")

    @pytest.mark.asyncio
    async def test_progress(self) -> None:
        results: list[int] = []

        async def append(x: int) -> None:
            results.append(x)

        await apfor(append, [1, 2, 3], progress="Running")
        assert sorted(results) == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_custom_workers(self) -> None:
        results: list[int] = []

        async def append(x: int) -> None:
            results.append(x)

        await apfor(append, [1, 2, 3], workers=2)
        assert sorted(results) == [1, 2, 3]
