"""qqabc.pipe — Pipeline 抽象層，需要 Python 3.10+."""

from __future__ import annotations

import sys

if sys.version_info < (3, 10):
    msg = (
        "qqabc.pipe requires Python 3.10 or later. "
        f"Current version: {sys.version_info[0]}.{sys.version_info[1]}"
    )
    raise ImportError(msg)

from qqabc.pipe.channel import AsyncBoundedQ, BoundedQ
from qqabc.pipe.pipeline import Pipeline, pipe
from qqabc.pipe.stage import ExecutorType, IStage, Stage

__all__ = [
    "AsyncBoundedQ",
    "BoundedQ",
    "ExecutorType",
    "IStage",
    "Pipeline",
    "Stage",
    "pipe",
]
