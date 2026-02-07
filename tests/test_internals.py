"""Tests for internal helper functions: _resolve_workers, _compute_chunksize, Config."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from parlane._config import Config
from parlane.api import _compute_chunksize, _resolve_workers


class TestResolveWorkers:
    """Tests for _resolve_workers() automatic worker selection."""

    def test_explicit_workers_returned_as_is(self) -> None:
        assert _resolve_workers(4, "thread", 100) == 4
        assert _resolve_workers(1, "process", 50) == 1
        assert _resolve_workers(16, "auto", 200) == 16

    def test_thread_backend_uses_cpu_plus_4(self) -> None:
        with patch("parlane.api.os.cpu_count", return_value=8):
            result = _resolve_workers(0, "thread", 1000)
            assert result == 12  # min(32, 8+4) = 12

    def test_process_backend_uses_cpu_count(self) -> None:
        with patch("parlane.api.os.cpu_count", return_value=8):
            result = _resolve_workers(0, "process", 1000)
            assert result == 8  # min(32, 8) = 8

    def test_capped_by_item_count(self) -> None:
        with patch("parlane.api.os.cpu_count", return_value=8):
            assert _resolve_workers(0, "thread", 3) == 3
            assert _resolve_workers(0, "process", 2) == 2

    def test_single_item(self) -> None:
        assert _resolve_workers(0, "thread", 1) == 1
        assert _resolve_workers(0, "process", 1) == 1

    def test_zero_items_returns_one(self) -> None:
        result = _resolve_workers(0, "thread", 0)
        assert result == 1  # max(1, 0) = 1

    def test_thread_capped_at_max_workers(self) -> None:
        with patch("parlane.api.os.cpu_count", return_value=64):
            result = _resolve_workers(0, "thread", 1000)
            assert result == 32  # min(32, 64+4) = 32

    def test_process_capped_at_max_workers(self) -> None:
        with patch("parlane.api.os.cpu_count", return_value=64):
            result = _resolve_workers(0, "process", 1000)
            assert result == 32  # min(32, 64) = 32

    def test_cpu_count_none_fallback(self) -> None:
        with patch("parlane.api.os.cpu_count", return_value=None):
            thread_result = _resolve_workers(0, "thread", 100)
            process_result = _resolve_workers(0, "process", 100)
            assert thread_result == 8  # min(32, 4+4) = 8
            assert process_result == 4  # min(32, 4) = 4

    def test_auto_backend_delegates_to_recommended(self) -> None:
        with (
            patch("parlane.api.recommended_backend", return_value="thread"),
            patch("parlane.api.os.cpu_count", return_value=8),
        ):
            result = _resolve_workers(0, "auto", 100)
            assert result == 12  # thread path: min(32, 8+4)

        with (
            patch("parlane.api.recommended_backend", return_value="process"),
            patch("parlane.api.os.cpu_count", return_value=8),
        ):
            result = _resolve_workers(0, "auto", 100)
            assert result == 8  # process path: min(32, 8)


class TestComputeChunksize:
    """Tests for _compute_chunksize() helper."""

    def test_zero_items(self) -> None:
        assert _compute_chunksize(0, 4) == 1

    def test_even_distribution(self) -> None:
        result = _compute_chunksize(100, 4)
        assert result >= 1
        assert result * 4 * 4 >= 100  # covers all items

    def test_small_input(self) -> None:
        result = _compute_chunksize(3, 4)
        assert result == 1

    def test_single_worker(self) -> None:
        result = _compute_chunksize(100, 1)
        assert result == 25  # 100 / (1*4) = 25

    def test_large_input(self) -> None:
        result = _compute_chunksize(10000, 8)
        assert result >= 1
        assert result <= 10000


class TestConfig:
    """Tests for Config dataclass validation."""

    def test_auto_workers(self) -> None:
        config = Config(workers=0)
        assert config.workers >= 1

    def test_auto_workers_with_cpu_count_none(self) -> None:
        with patch("parlane._config.os.cpu_count", return_value=None):
            config = Config(workers=0)
            assert config.workers == 4

    def test_auto_workers_capped_at_32(self) -> None:
        with patch("parlane._config.os.cpu_count", return_value=64):
            config = Config(workers=0)
            assert config.workers == 32

    def test_negative_workers_raises(self) -> None:
        with pytest.raises(ValueError, match="workers must be >= 1"):
            Config(workers=-1)

    def test_zero_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="timeout must be > 0"):
            Config(workers=1, timeout=0)

    def test_negative_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="timeout must be > 0"):
            Config(workers=1, timeout=-1.0)

    def test_zero_chunksize_raises(self) -> None:
        with pytest.raises(ValueError, match="chunksize must be >= 1"):
            Config(workers=1, chunksize=0)

    def test_negative_chunksize_raises(self) -> None:
        with pytest.raises(ValueError, match="chunksize must be >= 1"):
            Config(workers=1, chunksize=-5)
