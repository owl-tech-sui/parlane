"""Type aliases and generics for parlane."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, Literal, TypeVar

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")

BackendType = Literal["auto", "thread", "process"]
ErrorStrategy = Literal["raise", "skip", "collect"]


class Ok(Generic[T]):
    """Successful result wrapper."""

    __slots__ = ("value",)
    __match_args__ = ("value",)

    def __init__(self, value: T) -> None:
        self.value = value

    def is_ok(self) -> bool:
        return True

    def is_err(self) -> bool:
        return False

    def unwrap(self) -> T:
        return self.value

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Ok):
            return self.value == other.value  # type: ignore[no-any-return]
        return NotImplemented

    def __hash__(self) -> int:
        return hash(("Ok", self.value))


class Err:
    """Error result wrapper."""

    __slots__ = ("exception",)
    __match_args__ = ("exception",)

    def __init__(self, exception: BaseException) -> None:
        self.exception = exception

    def is_ok(self) -> bool:
        return False

    def is_err(self) -> bool:
        return True

    def unwrap(self) -> Any:
        raise self.exception

    def __repr__(self) -> str:
        return f"Err({self.exception!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Err):
            return type(self.exception) is type(other.exception) and str(
                self.exception
            ) == str(other.exception)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(("Err", type(self.exception), str(self.exception)))


Result = Ok[T] | Err

# Callable type aliases
MapFn = Callable[[T], U]
FilterFn = Callable[[T], bool]
ForFn = Callable[[T], Any]
