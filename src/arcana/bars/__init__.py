"""Bar construction layer — transforms raw trades into structured bars."""

from arcana.bars.base import Accumulator, Bar, BarBuilder
from arcana.bars.imbalance import (
    DollarImbalanceBarBuilder,
    TickImbalanceBarBuilder,
    VolumeImbalanceBarBuilder,
)
from arcana.bars.runs import (
    DollarRunBarBuilder,
    TickRunBarBuilder,
    VolumeRunBarBuilder,
)
from arcana.bars.standard import (
    DollarBarBuilder,
    TickBarBuilder,
    TimeBarBuilder,
    VolumeBarBuilder,
)
from arcana.bars.utils import EWMAEstimator, tick_rule

__all__ = [
    # Base
    "Accumulator",
    "Bar",
    "BarBuilder",
    # Standard (Prado Ch. 2 — fixed threshold)
    "DollarBarBuilder",
    "TickBarBuilder",
    "TimeBarBuilder",
    "VolumeBarBuilder",
    # Information-driven — Imbalance (Prado Ch. 2 — adaptive)
    "DollarImbalanceBarBuilder",
    "TickImbalanceBarBuilder",
    "VolumeImbalanceBarBuilder",
    # Information-driven — Run (Prado Ch. 2 — adaptive)
    "DollarRunBarBuilder",
    "TickRunBarBuilder",
    "VolumeRunBarBuilder",
    # Utilities
    "EWMAEstimator",
    "tick_rule",
]
