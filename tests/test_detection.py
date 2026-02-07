"""Tests for GIL detection logic."""

from __future__ import annotations

import sys
from unittest.mock import patch

from parlane._detection import is_gil_disabled, recommended_backend


class TestIsGilDisabled:
    """Tests for is_gil_disabled function."""

    def setup_method(self) -> None:
        # Clear the lru_cache before each test
        is_gil_disabled.cache_clear()

    def test_returns_bool(self) -> None:
        result = is_gil_disabled()
        assert isinstance(result, bool)

    def test_standard_python_returns_false(self) -> None:
        # On standard Python 3.10-3.12, GIL is always enabled
        if sys.version_info < (3, 13):
            assert is_gil_disabled() is False

    def test_with_runtime_check_gil_enabled(self) -> None:
        with patch.object(sys, "_is_gil_enabled", create=True, return_value=True):
            is_gil_disabled.cache_clear()
            assert is_gil_disabled() is False

    def test_with_runtime_check_gil_disabled(self) -> None:
        with patch.object(sys, "_is_gil_enabled", create=True, return_value=False):
            is_gil_disabled.cache_clear()
            assert is_gil_disabled() is True

    def test_cache_works(self) -> None:
        is_gil_disabled.cache_clear()
        result1 = is_gil_disabled()
        result2 = is_gil_disabled()
        assert result1 == result2
        info = is_gil_disabled.cache_info()
        assert info.hits >= 1


class TestRecommendedBackend:
    """Tests for recommended_backend function."""

    def setup_method(self) -> None:
        is_gil_disabled.cache_clear()

    def test_returns_string(self) -> None:
        result = recommended_backend()
        assert result in ("thread", "process")

    def test_gil_enabled_recommends_process(self) -> None:
        with patch.object(sys, "_is_gil_enabled", create=True, return_value=True):
            is_gil_disabled.cache_clear()
            assert recommended_backend() == "process"

    def test_gil_disabled_recommends_thread(self) -> None:
        with patch.object(sys, "_is_gil_enabled", create=True, return_value=False):
            is_gil_disabled.cache_clear()
            assert recommended_backend() == "thread"
