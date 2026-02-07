"""Progress bar support via tqdm (optional dependency).

tqdm is lazily imported — parlane core has zero extra dependencies.
Install with: pip install parlane[progress]
"""

from __future__ import annotations

from typing import Any


def resolve_progress(progress: bool | str) -> tuple[bool, str | None]:
    """Parse the progress parameter.

    Args:
        progress: False disables progress, True enables with no description,
                  a string enables with that description.

    Returns:
        (enabled, description) tuple.
    """
    if progress is False:
        return (False, None)
    if progress is True:
        return (True, None)
    return (True, str(progress))


def make_progress_bar(total: int, desc: str | None) -> Any:
    """Create a tqdm progress bar instance.

    Uses ``tqdm.auto`` for automatic terminal/Jupyter detection.

    Raises:
        ImportError: If tqdm is not installed.
    """
    try:
        from tqdm.auto import tqdm
    except ImportError:
        msg = (
            "tqdm is required for progress display. "
            "Install it with: pip install parlane[progress]"
        )
        raise ImportError(msg) from None

    return tqdm(total=total, desc=desc)
