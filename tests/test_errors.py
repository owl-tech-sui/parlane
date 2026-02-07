"""Tests for error types and Result types."""

from __future__ import annotations

import pytest

from parlane._errors import BackendError, ParlaneError, TaskError, TimeoutError
from parlane._types import Err, Ok


class TestOk:
    """Tests for Ok result wrapper."""

    def test_value(self) -> None:
        ok = Ok(42)
        assert ok.value == 42

    def test_is_ok(self) -> None:
        assert Ok(1).is_ok() is True

    def test_is_err(self) -> None:
        assert Ok(1).is_err() is False

    def test_unwrap(self) -> None:
        assert Ok("hello").unwrap() == "hello"

    def test_repr(self) -> None:
        assert repr(Ok(42)) == "Ok(42)"

    def test_equality(self) -> None:
        assert Ok(1) == Ok(1)
        assert Ok(1) != Ok(2)
        assert Ok(1) != "not an Ok"

    def test_hash(self) -> None:
        assert hash(Ok(1)) == hash(Ok(1))
        s = {Ok(1), Ok(1), Ok(2)}
        assert len(s) == 2


class TestErr:
    """Tests for Err result wrapper."""

    def test_exception(self) -> None:
        exc = ValueError("bad")
        err = Err(exc)
        assert err.exception is exc

    def test_is_ok(self) -> None:
        assert Err(ValueError()).is_ok() is False

    def test_is_err(self) -> None:
        assert Err(ValueError()).is_err() is True

    def test_unwrap_raises(self) -> None:
        exc = ValueError("test error")
        with pytest.raises(ValueError, match="test error"):
            Err(exc).unwrap()

    def test_repr(self) -> None:
        err = Err(ValueError("bad"))
        assert "Err" in repr(err)
        assert "ValueError" in repr(err)

    def test_equality(self) -> None:
        assert Err(ValueError("a")) == Err(ValueError("a"))
        assert Err(ValueError("a")) != Err(ValueError("b"))
        assert Err(ValueError("a")) != Err(TypeError("a"))
        assert Err(ValueError("a")) != "not an Err"

    def test_hash(self) -> None:
        assert hash(Err(ValueError("a"))) == hash(Err(ValueError("a")))
        s = {Err(ValueError("a")), Err(ValueError("a")), Err(ValueError("b"))}
        assert len(s) == 2


class TestTaskError:
    """Tests for TaskError."""

    def test_basic(self) -> None:
        original = ValueError("oops")
        err = TaskError("Task failed", original)
        assert err.original is original
        assert err.index is None
        assert "Task failed" in str(err)

    def test_with_index(self) -> None:
        original = ValueError("oops")
        err = TaskError("Task failed", original, index=5)
        assert err.index == 5

    def test_repr(self) -> None:
        err = TaskError("fail", ValueError("x"), index=3)
        r = repr(err)
        assert "TaskError" in r
        assert "index=3" in r

    def test_is_parlane_error(self) -> None:
        assert issubclass(TaskError, ParlaneError)


class TestTimeoutError:
    """Tests for TimeoutError."""

    def test_default_message(self) -> None:
        err = TimeoutError()
        assert "timed out" in str(err)

    def test_custom_message(self) -> None:
        err = TimeoutError("custom timeout")
        assert str(err) == "custom timeout"

    def test_is_parlane_error(self) -> None:
        assert issubclass(TimeoutError, ParlaneError)


class TestBackendError:
    """Tests for BackendError."""

    def test_is_parlane_error(self) -> None:
        assert issubclass(BackendError, ParlaneError)
