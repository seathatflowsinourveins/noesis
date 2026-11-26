"""Clean Tier-0 24/5 live Paper Farm automation package (`paper_only=True`)."""

from __future__ import annotations

from tier0_sota.live.paper_farm import (
    ALPACA_LANE_PROFILES,
    IBKR_PAPER_ACCOUNT,
    PAPER_ALPACA_ENDPOINT,
    AlpacaPaperLane,
    CrossLaneSymbolLock,
    IbkrPaperAdapter,
    PaperFarmCoordinator,
    select_short_or_inverse_fallback,
)
from tier0_sota.live.performance import (
    PaperPerformanceAggregator,
    aggregate_paper_farm_performance,
    build_finos_openbb_export,
)
from tier0_sota.live.scanner import (
    DEFAULT_PIN_SYMBOLS,
    ExtremeMoverScanner,
    aggregate_finra_trf_ratios,
    lookup_alpaca_paper_borrow,
    scan_extreme_movers,
)

__all__ = [
    "ALPACA_LANE_PROFILES",
    "DEFAULT_PIN_SYMBOLS",
    "IBKR_PAPER_ACCOUNT",
    "PAPER_ALPACA_ENDPOINT",
    "AlpacaPaperLane",
    "CrossLaneSymbolLock",
    "ExtremeMoverScanner",
    "IbkrPaperAdapter",
    "PaperFarmCoordinator",
    "PaperPerformanceAggregator",
    "aggregate_finra_trf_ratios",
    "aggregate_paper_farm_performance",
    "build_finos_openbb_export",
    "lookup_alpaca_paper_borrow",
    "scan_extreme_movers",
    "select_short_or_inverse_fallback",
]
