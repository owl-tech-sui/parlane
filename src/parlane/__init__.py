"""parlane — Dead-simple parallel data processing for Python.

Usage:
    >>> from parlane import pmap, pfilter, pfor
    >>> results = pmap(lambda x: x ** 2, range(100))
"""

from __future__ import annotations

from parlane._async import apfilter, apfor, apmap
from parlane._detection import is_gil_disabled, recommended_backend
from parlane._errors import BackendError, ParlaneError, TaskError, TimeoutError
from parlane._pipeline import Pipeline, pipeline
from parlane._types import BackendType, Err, ErrorStrategy, Ok, Result
from parlane.api import pfilter, pfor, pmap, pstarmap

try:
    from parlane._version import __version__
except ImportError:  # pragma: no cover
    __version__ = "0.0.0.dev0"

__all__ = [
    "BackendError",
    "BackendType",
    "Err",
    "ErrorStrategy",
    "Ok",
    "ParlaneError",
    "Pipeline",
    "Result",
    "TaskError",
    "TimeoutError",
    "__version__",
    "apfilter",
    "apfor",
    "apmap",
    "is_gil_disabled",
    "pfilter",
    "pfor",
    "pipeline",
    "pmap",
    "pstarmap",
    "recommended_backend",
]
