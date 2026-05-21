from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import _number, _safety


ALIAS_PATTERNS: dict[str, list[str]] = {
    "BTCUSD": ["BTCUSD", "BTCUSDm", "BTCUSD.r", "BTCUSD.b", "BTCUSD#", "BTCUSD.a", "BTCUSD.raw"],
    "ETHUSD": ["ETHUSD", "ETHUSDm", "ETHUSD.r", "ETHUSD.b", "ETHUSD#", "ETHUSD.a", "ETHUSD.raw"],
    "XAUUSD": ["XAUUSD", "XAUUSDm", "XAUUSD.r", "XAUUSD.b", "GOLD", "GOLDm", "GOLD.r", "GOLD.b", "XAUUSD#"],
    "NAS100": ["NAS100", "NAS100m", "NAS100.r", "NAS100.b", "US100", "US100m", "US100.b", "USTEC", "USTECm", "USTEC.b", "NASDAQ", "NASDAQm"],
    "US500": ["US500", "US500m", "US500.r", "US500.b", "SPX500", "SPX500m", "SPX500.b", "SP500", "SP500m", "USSPX500"],
    "EURUSD": ["EURUSD", "EURUSDm", "EURUSD.r", "EURUSD.b", "EURUSD#", "EURUSD.a", "EURUSD.raw"],
    "GBPUSD": ["GBPUSD", "GBPUSDm", "GBPUSD.r", "GBPUSD.b", "GBPUSD#", "GBPUSD.a", "GBPUSD.raw"],
}


@dataclass(frozen=True)
class SymbolCostModel:
    requested_symbol: str
    resolved_symbol: str
    instrument_type: str
    digits: int
    point: float
    tick_size: float
    spread_points: float
    estimated_spread_price: float
    commission_assumption: float
    slippage_assumption: float
    spread_x1_5_cost: float
    spread_x2_cost: float
    cost_model_confidence: str
    cost_model_reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "requested_symbol": self.requested_symbol,
            "resolved_symbol": self.resolved_symbol,
            "instrument_type": self.instrument_type,
            "digits": self.digits,
            "point": self.point,
            "tick_size": self.tick_size,
            "spread_points": self.spread_points,
            "estimated_spread_price": self.estimated_spread_price,
            "commission_assumption": self.commission_assumption,
            "slippage_assumption": self.slippage_assumption,
            "spread_x1_5_cost": self.spread_x1_5_cost,
            "spread_x2_cost": self.spread_x2_cost,
            "cost_model_confidence": self.cost_model_confidence,
            "cost_model_reason": self.cost_model_reason,
        }


def build_symbol_cost_model(
    requested_symbol: str,
    *,
    resolved_symbol: str | None = None,
    first_price: float | None = None,
    digits: int | None = None,
    point: float | None = None,
    tick_size: float | None = None,
    broker_spread_points: float | None = None,
) -> SymbolCostModel:
    requested = str(requested_symbol or "").upper().strip()
    resolved = str(resolved_symbol or requested).upper().strip() or requested
    instrument_type = infer_instrument_type(resolved or requested)
    price = float(first_price or 0.0)
    inferred_digits = int(digits if digits is not None else _default_digits(instrument_type, resolved))
    inferred_point = float(point if point is not None else _default_point(instrument_type, inferred_digits))
    inferred_tick = float(tick_size if tick_size is not None else inferred_point)
    spread_points = float(broker_spread_points if broker_spread_points is not None else _default_spread_points(instrument_type, resolved, price, inferred_point))
    estimated_spread_price = round(spread_points * inferred_point, 10)
    confidence, reason = _confidence(instrument_type, price, broker_spread_points, estimated_spread_price)
    commission = _default_commission(instrument_type)
    slippage = _default_slippage(instrument_type, spread_points)
    return SymbolCostModel(
        requested_symbol=requested,
        resolved_symbol=resolved,
        instrument_type=instrument_type,
        digits=inferred_digits,
        point=round(inferred_point, 10),
        tick_size=round(inferred_tick, 10),
        spread_points=round(spread_points, 6),
        estimated_spread_price=estimated_spread_price,
        commission_assumption=commission,
        slippage_assumption=round(slippage, 6),
        spread_x1_5_cost=round(spread_points * 1.5, 6),
        spread_x2_cost=round(spread_points * 2.0, 6),
        cost_model_confidence=confidence,
        cost_model_reason=reason,
    )


def infer_instrument_type(symbol: str) -> str:
    clean = "".join(char for char in str(symbol or "").upper() if char.isalnum())
    if clean.startswith(("BTC", "ETH")):
        return "crypto"
    if clean.startswith(("XAU", "GOLD")):
        return "metal"
    if any(token in clean for token in ["NAS", "US100", "USTEC", "NASDAQ", "US500", "SPX", "SP500"]):
        return "index"
    if len(clean) >= 6 and clean[:3].isalpha() and clean[3:6].isalpha():
        return "forex"
    return "unknown"


def discover_alias(requested_symbol: str, available_symbols: list[str] | set[str]) -> dict[str, Any]:
    requested = str(requested_symbol or "").upper().strip()
    available = list(available_symbols)
    by_upper = {symbol.upper(): symbol for symbol in available}
    patterns = ALIAS_PATTERNS.get(requested, [requested])
    for pattern in patterns:
        hit = by_upper.get(pattern.upper())
        if hit:
            return {"requested_symbol": requested, "resolved_symbol": hit, "matched_alias": pattern, "status": "resolved_exact"}
    requested_root = _root_token(requested)
    for symbol in available:
        if _root_token(symbol).startswith(requested_root) or requested_root.startswith(_root_token(symbol)):
            return {"requested_symbol": requested, "resolved_symbol": symbol, "matched_alias": symbol, "status": "resolved_fuzzy"}
    return {"requested_symbol": requested, "resolved_symbol": "", "matched_alias": "", "status": "not_found"}


def make_cost_model_report(symbols: list[str], csv_dir: Path | str, *, output_dir: Path | str | None = None) -> dict[str, Any]:
    root = Path(csv_dir)
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        path = _find_any_csv(root, symbol)
        first_price = _first_price(path) if path else 0.0
        model = build_symbol_cost_model(symbol, first_price=first_price)
        row = {**model.as_dict(), "csv_path": str(path or ""), "csv_found": bool(path), **_safety()}
        rows.append(row)
    result = {
        "ok": True,
        "status": "mt5_symbol_cost_model_report_ready",
        "rows": rows,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    if output_dir is not None:
        write_cost_model_report(result, output_dir)
    return result


def write_cost_model_report(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "multi_symbol_cost_model_report.csv"
    json_path = root / "multi_symbol_cost_model_report.json"
    rows = result.get("rows") if isinstance(result.get("rows"), list) else []
    headers = [
        "requested_symbol",
        "resolved_symbol",
        "instrument_type",
        "digits",
        "point",
        "tick_size",
        "spread_points",
        "estimated_spread_price",
        "commission_assumption",
        "slippage_assumption",
        "spread_x1_5_cost",
        "spread_x2_cost",
        "cost_model_confidence",
        "cost_model_reason",
        "csv_found",
        "csv_path",
        "broker_touched",
        "order_executed",
        "order_policy",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    return csv_path, json_path


def _default_digits(instrument_type: str, symbol: str) -> int:
    if instrument_type == "forex":
        return 3 if "JPY" in symbol else 5
    if instrument_type == "crypto":
        return 2
    if instrument_type == "metal":
        return 2
    if instrument_type == "index":
        return 1
    return 2


def _default_point(instrument_type: str, digits: int) -> float:
    if instrument_type == "forex":
        return 10 ** (-digits)
    if instrument_type == "index":
        return 0.1 if digits <= 1 else 10 ** (-digits)
    return 10 ** (-digits)


def _default_spread_points(instrument_type: str, symbol: str, price: float, point: float) -> float:
    if instrument_type == "forex":
        pip = 0.01 if "JPY" in symbol else 0.0001
        spread_price = 1.2 * pip
        return max(1.0, spread_price / max(point, 0.00000001))
    if instrument_type == "crypto":
        spread_price = max(price * 0.00018, 0.5)
        return max(1.0, spread_price / max(point, 0.00000001))
    if instrument_type == "metal":
        return max(1.0, 0.25 / max(point, 0.00000001))
    if instrument_type == "index":
        return max(1.0, 1.5 / max(point, 0.00000001))
    return 10.0


def _default_commission(instrument_type: str) -> float:
    if instrument_type == "forex":
        return 0.0
    if instrument_type == "crypto":
        return 0.0
    return 0.0


def _default_slippage(instrument_type: str, spread_points: float) -> float:
    if instrument_type == "forex":
        return min(max(spread_points * 0.15, 0.2), 3.0)
    if instrument_type == "crypto":
        return min(max(spread_points * 0.20, 1.0), spread_points)
    if instrument_type in {"metal", "index"}:
        return min(max(spread_points * 0.20, 1.0), spread_points)
    return min(max(spread_points * 0.20, 1.0), spread_points)


def _confidence(instrument_type: str, price: float, broker_spread_points: float | None, estimated_spread_price: float) -> tuple[str, str]:
    if broker_spread_points is not None:
        return "high", "broker_spread_points_available"
    if instrument_type == "unknown":
        return "low", "unknown_instrument_type"
    if price <= 0:
        return "medium", "default_model_without_price"
    if estimated_spread_price <= 0:
        return "low", "invalid_estimated_spread"
    return "medium", "default_instrument_model"


def _find_any_csv(root: Path, symbol: str) -> Path | None:
    requested = str(symbol or "").upper().strip()
    for candidate in dict.fromkeys([symbol, requested, *ALIAS_PATTERNS.get(requested, [])]):
        for path in sorted(root.glob(f"{candidate}_*.csv")):
            if path.is_file():
                return path
    return None


def _first_price(path: Path | None) -> float:
    if path is None:
        return 0.0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                return float(_number(row.get("close")) or _number(row.get("open")) or 0.0)
    except Exception:
        return 0.0
    return 0.0


def _root_token(symbol: str) -> str:
    clean = "".join(char for char in str(symbol or "").upper() if char.isalnum())
    for suffix in ["M", "R", "RAW", "A"]:
        if clean.endswith(suffix) and len(clean) > len(suffix) + 3:
            clean = clean[: -len(suffix)]
    return clean
