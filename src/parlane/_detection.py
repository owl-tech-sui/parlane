"""GIL detection logic.

Detection strategy (ordered by reliability):
1. sys._is_gil_enabled()  -- runtime GIL state (3.13+)
2. sysconfig.get_config_var('Py_GIL_DISABLED') -- build-time flag (3.13+)
3. Fallback: assume GIL is enabled (3.10-3.12)
"""

from __future__ import annotations

import sys
import sysconfig
from functools import lru_cache


@lru_cache(maxsize=1)
def is_gil_disabled() -> bool:
    """Detect whether the GIL is disabled at runtime.

    Returns True if the current Python interpreter is running without
    the GIL (free-threaded mode). Returns False otherwise.
    """
    # Strategy 1: Runtime check (Python 3.13+)
    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if is_gil_enabled is not None:
        return not is_gil_enabled()

    # Strategy 2: Build-time check (Python 3.13+)
    gil_disabled = sysconfig.get_config_var("Py_GIL_DISABLED")
    if gil_disabled is not None:
        return bool(int(gil_disabled))

    # Strategy 3: Fallback for Python 3.10-3.12
    return False


def recommended_backend() -> str:
    """Return the recommended backend based on GIL state.

    Returns "thread" if GIL is disabled (true parallelism via threads),
    "process" otherwise (bypass GIL via multiprocessing).
    """
    return "thread" if is_gil_disabled() else "process"
