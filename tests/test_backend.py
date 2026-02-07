"""Tests for backend creation and execution."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from parlane._backend import (
    ProcessBackend,
    ThreadBackend,
    _get_mp_context,
    create_backend,
)
from parlane._errors import BackendError


class TestCreateBackend:
    """Tests for create_backend factory."""

    def test_thread_backend(self) -> None:
        be = create_backend("thread", 2)
        assert isinstance(be, ThreadBackend)
        be.shutdown()

    def test_process_backend(self) -> None:
        be = create_backend("process", 2)
        assert isinstance(be, ProcessBackend)
        be.shutdown()

    def test_auto_backend(self) -> None:
        be = create_backend("auto", 2)
        assert isinstance(be, (ThreadBackend, ProcessBackend))
        be.shutdown()

    def test_invalid_backend(self) -> None:
        with pytest.raises(BackendError, match="Unknown backend type"):
            create_backend("invalid", 2)  # type: ignore[arg-type]


def _square(x: int) -> int:
    return x * x


class TestThreadBackend:
    """Tests for ThreadBackend."""

    def test_map(self) -> None:
        with ThreadBackend(2) as be:
            result = list(be.map(_square, iter(range(10))))
        assert result == [x * x for x in range(10)]

    def test_submit(self) -> None:
        with ThreadBackend(2) as be:
            future = be.submit(_square, 7)
            assert future.result() == 49

    def test_context_manager(self) -> None:
        with ThreadBackend(2) as be:
            result = list(be.map(_square, iter([1, 2, 3])))
        assert result == [1, 4, 9]


class TestProcessBackend:
    """Tests for ProcessBackend."""

    def test_map(self) -> None:
        with ProcessBackend(2) as be:
            result = list(be.map(_square, iter(range(10))))
        assert result == [x * x for x in range(10)]

    def test_submit(self) -> None:
        with ProcessBackend(2) as be:
            future = be.submit(_square, 7)
            assert future.result() == 49

    def test_context_manager(self) -> None:
        with ProcessBackend(2) as be:
            result = list(be.map(_square, iter([1, 2, 3])))
        assert result == [1, 4, 9]


class TestGetMpContext:
    """Tests for _get_mp_context helper."""

    def test_windows_returns_none(self) -> None:
        with patch("parlane._backend.sys.platform", "win32"):
            assert _get_mp_context() is None

    def test_non_windows_returns_fork(self) -> None:
        mock_ctx = object()
        with (
            patch("parlane._backend.sys.platform", "darwin"),
            patch("parlane._backend.mp.get_context", return_value=mock_ctx) as mock_get,
        ):
            ctx = _get_mp_context()
            assert ctx is mock_ctx
            mock_get.assert_called_once_with("fork")
