"""Whole-market live extreme-mover scanner (Alpaca SIP + FINRA TRF + ETB lookup)."""

from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import alpaca.data as ad
import alpaca.trading as at
from alpaca.data.enums import DataFeed, Exchange, MostActivesBy
from alpaca.data.requests import (
    MarketMoversRequest,
    MostActivesRequest,
    StockBarsRequest,
    StockTradesRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.enums import AssetBorrowStatus, AssetStatus
from alpaca.trading.requests import GetAssetsRequest

from tier0_sota import engine as eng
from tier0_sota import risk as rk

DEFAULT_PIN_SYMBOLS: tuple[str, ...] = ("SOXL", "SOXS")
VALID_EQUITY_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,6}$")
_UNVERIFIED = {
    "shortable": False,
    "easy_to_borrow": False,
    "borrow_source": None,
    "borrow_as_of_utc": None,
    "short_fee_bps": 30.0,
}


def _ga(obj: Any, key: str, default: Any = None) -> Any:
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def _f(val: Any, default: float = 0.0) -> float:
    try:
        out = float(val)
        return out if math.isfinite(out) else default
    except (TypeError, ValueError):
        return default


def resolve_alpaca_env_creds(account: str = "ALPACA_PA1") -> tuple[str, str]:
    """Read Alpaca Paper credentials strictly from process env (never opens .env*)."""
    idx = account.removeprefix("ALPACA_PA")
    k_names = [f"ALPACA_PA{idx}_API_KEY"] + (["APCA_API_KEY_ID"] if idx == "1" else [])
    s_names = [f"ALPACA_PA{idx}_SECRET", f"ALPACA_PA{idx}_SECRET_KEY"] + (
        ["APCA_API_SECRET_KEY"] if idx == "1" else []
    )
    key = next((v for k in k_names if (v := os.environ.get(k, "").strip())), "")
    sec = next((v for s in s_names if (v := os.environ.get(s, "").strip())), "")
    if key.startswith("AK"):
        raise PermissionError(f"Refused potential Live AK key for {account}; Paper keys only.")
    return key, sec


def aggregate_finra_trf_ratios(trades: Sequence[Any]) -> dict[str, dict[str, float]]:
    """Compute FINRA TRF (Exchange.D) darkpool share from Alpaca SIP trades."""
    out: dict[str, dict[str, float]] = {}
    for tr in trades:
        sym = str(_ga(tr, "symbol", "") or "").strip().upper()
        sz = _f(_ga(tr, "size"), 0.0)
        if not sym or sz <= 0.0:
            continue
        rec = out.setdefault(
            sym, {"trf_volume": 0.0, "total_volume": 0.0, "darkpool_trf_ratio": 0.0}
        )
        rec["total_volume"] += sz
        ex = _ga(tr, "exchange")
        if str(_ga(ex, "value", ex) or "").strip().upper() == Exchange.D.value:
            rec["trf_volume"] += sz
        rec["darkpool_trf_ratio"] = round(rec["trf_volume"] / max(1.0, rec["total_volume"]), 4)
    return out


def lookup_alpaca_paper_borrow(
    symbols: Sequence[str], trading_client: Any, *, as_of: datetime | None = None
) -> dict[str, dict[str, Any]]:
    """Post-selection `alpaca_paper_asset` shortable & ETB borrow verification."""
    reqs = sorted({s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()})
    if not reqs or trading_client is None:
        return {}
    now_iso = (as_of or datetime.now(UTC)).isoformat()
    raw = (
        trading_client.get_all_assets(GetAssetsRequest(status=AssetStatus.ACTIVE))
        if len(reqs) > 10
        else [trading_client.get_asset(s) for s in reqs]
    )
    out: dict[str, dict[str, Any]] = {}
    for asset in raw:
        sym = str(_ga(asset, "symbol", "") or "").strip().upper()
        if sym not in reqs:
            continue
        sh, bs = bool(_ga(asset, "shortable", False)), _ga(asset, "borrow_status")
        etb = (
            bs == AssetBorrowStatus.EASY_TO_BORROW
            or str(bs).strip().lower().endswith("easy_to_borrow")
            if bs is not None
            else bool(_ga(asset, "easy_to_borrow", False))
        )
        out[sym] = {
            "shortable": sh,
            "easy_to_borrow": sh and etb,
            "borrow_source": "alpaca_paper_asset",
            "borrow_as_of_utc": now_iso,
            "short_fee_bps": 0.0 if (sh and etb) else 30.0,
        }
    return out


@dataclass(frozen=True)
class ExtremeMoverScanner:
    """Alpaca SIP + FINRA TRF whole-market extreme-mover scanner (zero banned libs)."""

    screener_client: Any | None = None
    data_client: Any | None = None
    trading_client: Any | None = None
    pin_symbols: tuple[str, ...] = DEFAULT_PIN_SYMBOLS
    min_move_pct: float = 3.5
    min_rvol: float = 1.5
    min_price: float = 1.50
    min_dollar_vol: float = 5_000_000.0

    @classmethod
    def from_env(cls, account: str = "ALPACA_PA1", **kw: Any) -> ExtremeMoverScanner:
        key, sec = resolve_alpaca_env_creds(account)
        if not (key and sec):
            raise RuntimeError(f"Missing Alpaca Paper credentials in env for {account}")
        return cls(
            screener_client=ad.ScreenerClient(key, sec),
            data_client=ad.StockHistoricalDataClient(key, sec),
            trading_client=at.TradingClient(key, sec, paper=True),
            **kw,
        )

    def _discover_raw(self) -> dict[str, dict[str, Any]]:
        cands: dict[str, dict[str, Any]] = {}
        if self.screener_client is not None:
            act = self.screener_client.get_most_actives(
                MostActivesRequest(top=30, by=MostActivesBy.VOLUME)
            )
            mov = self.screener_client.get_market_movers(MarketMoversRequest(top=30))
            for src, attr, tag in (
                (act, "most_actives", "alpaca_most_actives"),
                (mov, "gainers", "alpaca_gainers"),
                (mov, "losers", "alpaca_losers"),
            ):
                for item in _ga(src, attr) or ():
                    sym = str(_ga(item, "symbol", "") or "").strip().upper()
                    if not sym or not VALID_EQUITY_RE.match(sym):
                        continue
                    px, vol, chg = (
                        _f(_ga(item, "price")),
                        _f(_ga(item, "volume")),
                        _f(_ga(item, "percent_change")),
                    )
                    rec = cands.setdefault(
                        sym, {"symbol": sym, "price": 0.0, "volume": 0.0, "move": 0.0, "tags": []}
                    )
                    rec["price"], rec["volume"] = max(rec["price"], px), max(rec["volume"], vol)
                    if abs(chg) > abs(rec["move"]):
                        rec["move"] = chg
                    if tag not in rec["tags"]:
                        rec["tags"].append(tag)
        for pin in self.pin_symbols:
            if (p := pin.strip().upper()) and VALID_EQUITY_RE.match(p):
                cands.setdefault(
                    p, {"symbol": p, "price": 0.0, "volume": 0.0, "move": 0.0, "tags": ["pinned"]}
                )
        return cands

    def _enrich_sip(
        self, syms: Sequence[str], now_utc: datetime
    ) -> tuple[dict[str, list[Any]], dict[str, float]]:
        if self.data_client is None or not syms:
            return {}, {}
        bars_r = self.data_client.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=sorted({*syms, "SPY", "QQQ"}),
                timeframe=TimeFrame.Minute,
                start=now_utc - timedelta(hours=6),
                end=now_utc,
                feed=DataFeed.SIP,
            )
        )
        trades_r = self.data_client.get_stock_trades(
            StockTradesRequest(
                symbol_or_symbols=list(syms)[:25],
                start=now_utc - timedelta(hours=4),
                feed=DataFeed.SIP,
                limit=2000,
            )
        )
        flat: list[Any] = []
        for sym, t_list in (_ga(trades_r, "data") or {}).items():
            for tr in t_list or ():
                if not _ga(tr, "symbol"):
                    tr.symbol = sym
                flat.append(tr)
        trf = {s: r["darkpool_trf_ratio"] for s, r in aggregate_finra_trf_ratios(flat).items()}
        return dict(_ga(bars_r, "data") or {}), trf

    def scan(
        self, *, top_k: int = 15, output_path: Path | None = None, now_utc: datetime | None = None
    ) -> dict[str, Any]:
        """Run whole-market Alpaca SIP extreme-mover scan + post-selection ETB lookup."""
        now = now_utc or datetime.now(UTC)
        cands = self._discover_raw()
        pins = {p.strip().upper() for p in self.pin_symbols if p.strip()}
        bars_map, trf_ratios = self._enrich_sip(sorted(cands), now)
        spy = bars_map.get("SPY") or []
        spy_mv = (
            ((float(spy[-1].close) / float(spy[0].open)) - 1.0) * 100.0
            if len(spy) >= 2 and float(spy[0].open) > 0
            else 0.0
        )
        macro_regime = 1 if spy_mv >= 0.25 else (-1 if spy_mv <= -0.75 else 0)
        sess = eng.classify_us_equity_session_et(int(now.timestamp() * 1_000_000_000))

        scored: list[dict[str, Any]] = []
        for sym, meta in cands.items():
            sb = bars_map.get(sym) or []
            px = float(sb[-1].close) if sb else float(meta["price"])
            if px < self.min_price and sym not in pins:
                continue
            bmv = (
                ((float(sb[-1].close) / float(sb[0].open)) - 1.0) * 100.0
                if len(sb) >= 2 and float(sb[0].open) > 0
                else 0.0
            )
            mv = meta["move"] if abs(meta["move"]) > 0.0 else bmv
            vol = max(float(meta["volume"]), sum(_f(_ga(b, "volume")) for b in sb))
            base_v = sum(_f(_ga(b, "volume")) for b in sb[:-5]) / max(1, len(sb) - 5)
            rec_v = sum(_f(_ga(b, "volume")) for b in sb[-5:]) / 5.0 if sb else 0.0
            rvol = (
                (rec_v / base_v)
                if base_v > 0
                else (2.0 if abs(mv) >= self.min_move_pct else 1.0)
            )
            is_ext = (
                abs(mv) >= self.min_move_pct
                and rvol >= self.min_rvol
                and (px * vol >= self.min_dollar_vol or sym in pins)
            )
            if not is_ext and sym not in pins:
                continue  # reject flat/sleepy mega-caps (e.g. flat AAPL)
            side: rk.Side = "SHORT" if mv <= -self.min_move_pct else "LONG"
            sched, sp_map = eng.select_pooled_trailing_schedule(Decimal("0.015"))
            prof = eng.get_session_protection_profile(sess, sp_map)
            stop_px = None if sess == "CLOSED" or px <= 0 else float(
                rk.compute_capped_stop_price(
                    Decimal(f"{px:.4f}"), side, max_loss_pct=prof.initial_max_loss_pct
                )
            )
            sc = abs(mv) * 2.0 + math.log1p(max(0.0, rvol)) * 10.0 + trf_ratios.get(sym, 0.0) * 8.0
            scored.append({
                "symbol": sym, "score": round(sc + (20.0 if sym in pins else 0.0), 2),
                "last_price": round(px, 4), "session_move_pct": round(mv, 2),
                "rvol_normalized": round(rvol, 2), "dollar_vol": round(px * vol, 2),
                "darkpool_trf_ratio": trf_ratios.get(sym),
                "side": "SELL" if side == "SHORT" else "BUY", "session": sess,
                "trailing_schedule": sched, "initial_stop_px": stop_px,
                "extreme_mover_eligible": is_ext, "pinned": sym in pins,
                "screen_buckets": list(meta["tags"]),
            })

        scored.sort(key=lambda r: (-float(r["score"]), str(r["symbol"])))
        selected = scored[: max(1, int(top_k))]
        b_map = lookup_alpaca_paper_borrow(
            [r["symbol"] for r in selected], self.trading_client, as_of=now
        )
        entries = [
            row | b_map.get(row["symbol"], _UNVERIFIED) | {"rank": i}
            for i, row in enumerate(selected, 1)
        ]
        manifest = {
            "schema_version": "tier0.whole-market-movers.v2",
            "generated_at_utc": now.isoformat(),
            "feed_sources": ["alpaca_sip", "finra_trf_exchange_d"],
            "macro_regime": {"regime": macro_regime, "spy_session_pct": round(spy_mv, 3)},
            "pin_symbols": list(self.pin_symbols), "ranked_count": len(entries), "entries": entries,
        }
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            tmp.replace(output_path)
        return manifest


def scan_extreme_movers(**kw: Any) -> dict[str, Any]:
    """Convenience entrypoint running `ExtremeMoverScanner.from_env().scan(...)`."""
    acct = str(kw.pop("account", "ALPACA_PA1"))
    top_k, out = int(kw.pop("top_k", 15)), kw.pop("output_path", None)
    return ExtremeMoverScanner.from_env(account=acct, **kw).scan(top_k=top_k, output_path=out)
