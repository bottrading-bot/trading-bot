"""Long-only Alpaca live trading bot. Live orders require an explicit env switch."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.enums import DataFeed
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetCalendarRequest, GetOrdersRequest, MarketOrderRequest

load_dotenv()

# Capital estimate: 70 EUR * ECB EUR/USD reference rate 1.1133 on 2026-10-28.
# The broker's actual USD cash balance is authoritative and will usually be lower
# after FX conversion/transfer costs. The bot never uses buying power or margin.
CAPITAL_CAP_USD = float(os.getenv("CAPITAL_CAP_USD", "77.93"))
POSITION_CASH_FRACTION = 0.90
MAX_DAILY_LOSS_PCT = 0.05
MAX_ROUND_TRIPS_IN_5_SESSIONS = 3  # Conservative guard for accounts still on legacy controls.
ENTRY_SCORE_THRESHOLD = 4
POLL_SECONDS = 60
MIN_NOTIONAL_USD = 1.00
STOP_LOSS_PCT = 0.03
TRAILING_STOP_PCT = 0.025
TRAILING_STOP_AFTER_PROFIT_PCT = 0.015
TRAILING_ACTIVATION_PCT = 0.03
TAKE_PROFIT_PCT = 0.05
NY = ZoneInfo("America/New_York")
WATCHLIST = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "PLTR", "SNOW", "COIN", "RIOT", "MARA", "SPY", "QQQ"]
ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "bot_state.json"
LOG_FILE = ROOT / "bot.log"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger("trading_bot")


def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def make_clients() -> tuple[TradingClient, StockHistoricalDataClient]:
    key = os.getenv("ALPACA_API_KEY", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError("Neue Alpaca-LIVE-Schlüssel fehlen in .env")
    # This bot intentionally targets the real brokerage account, never paper.
    return (TradingClient(key, secret, paper=False), StockHistoricalDataClient(key, secret))


client: TradingClient | None = None
market_data_client: StockHistoricalDataClient | None = None
state = {"session_date": None, "day_start_equity": 0.0, "active_symbol": None,
         "entry_date": None, "entry_price": 0.0, "peak": 0.0,
         "exit_pending": False, "round_trips": []}


def load_state() -> None:
    global state
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                state.update(saved)
        except (OSError, json.JSONDecodeError):
            raise RuntimeError("bot_state.json ist unlesbar. Bot stoppt, bis der State geprüft wurde.")


def save_state() -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def account_snapshot() -> tuple[float, float]:
    assert client is not None
    account = client.get_account()
    status = str(account.status).lower()
    if "active" not in status or bool(account.trading_blocked):
        raise RuntimeError(f"Live-Konto nicht handelbar (status={account.status}, trading_blocked={account.trading_blocked})")
    return float(account.equity), float(account.cash)


def update_daily_baseline() -> None:
    equity, _ = account_snapshot()
    today = dt.datetime.now(NY).date().isoformat()
    if state.get("session_date") != today:
        state["session_date"] = today
        state["day_start_equity"] = equity
        save_state()
    elif float(state.get("day_start_equity", 0)) <= 0:
        state["day_start_equity"] = equity
        save_state()


def daily_loss_hit() -> bool:
    equity, _ = account_snapshot()
    start = float(state.get("day_start_equity", 0))
    return start > 0 and (start - equity) / start >= MAX_DAILY_LOSS_PCT


def recent_round_trip_count() -> int:
    assert client is not None
    today = dt.datetime.now(NY).date()
    # Use Alpaca's exchange calendar so holidays and early-week sessions are
    # counted as trading sessions, not guessed from weekdays.
    try:
        calendar = client.get_calendar(filters=GetCalendarRequest(
            start=today - dt.timedelta(days=14), end=today
        ))
        session_dates = [item.date.isoformat() for item in calendar]
        window = set(session_dates[-5:])
    except Exception:
        log.exception("Cannot read Alpaca calendar; blocking new entry conservatively.")
        return MAX_ROUND_TRIPS_IN_5_SESSIONS
    rows = state.get("round_trips", [])
    kept = []
    for row in rows:
        try:
            date = dt.date.fromisoformat(row["date"])
        except (TypeError, ValueError, KeyError):
            continue
        if date.isoformat() in window:
            kept.append(row)
    state["round_trips"] = kept
    save_state()
    return len(kept)


def latest_bars(symbol: str, days: int = 10) -> pd.DataFrame:
    assert market_data_client is not None
    end = dt.datetime.now(dt.timezone.utc)
    request = StockBarsRequest(symbol_or_symbols=symbol,
                               timeframe=TimeFrame(5, TimeFrameUnit.Minute),
                               start=end - dt.timedelta(days=days), end=end,
                               feed=DataFeed.IEX)
    bars = market_data_client.get_stock_bars(request).df
    if bars.empty:
        return pd.DataFrame()
    if isinstance(bars.index, pd.MultiIndex):
        try:
            bars = bars.xs(symbol, level="symbol")
        except KeyError:
            return pd.DataFrame()
    bars = bars.sort_index().copy()
    completed_bar_cutoff = end.replace(minute=(end.minute // 5) * 5, second=0, microsecond=0)
    bars = bars[bars.index < completed_bar_cutoff]
    if bars.empty:
        return pd.DataFrame()
    close = bars["close"].astype(float)
    bars["return"] = close.pct_change()
    bars["ma20"] = close.rolling(20).mean()
    bars["ma50"] = close.rolling(50).mean()
    bars["ema9"] = close.ewm(span=9, adjust=False).mean()
    delta = close.diff()
    avg_gain = delta.clip(lower=0).rolling(14).mean()
    avg_loss = -delta.clip(upper=0).rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    bars["rsi14"] = 100 - 100 / (1 + rs)
    bars["vol20"] = bars["volume"].astype(float).rolling(20).mean()
    return bars.dropna()


def signal(frame: pd.DataFrame) -> tuple[int, list[str]]:
    if len(frame) < 60:
        return 0, []
    current, previous = frame.iloc[-1], frame.iloc[-2]
    criteria = [
        (current.ma20 > current.ma50, "trend"),
        (current.ema9 > current.ma20, "ema"),
        (current["return"] > 0.002 and previous["return"] > 0, "momentum"),
        (48 <= current.rsi14 <= 72, "rsi"),
        (current.volume > current.vol20, "volume"),
    ]
    reasons = [name for passed, name in criteria if bool(passed)]
    return len(reasons), reasons


def choose_candidate() -> dict | None:
    market = latest_bars("SPY")
    if market.empty or market.iloc[-1].ma20 <= market.iloc[-1].ma50:
        return None
    best = None
    for symbol in WATCHLIST:
        frame = latest_bars(symbol)
        score, reasons = signal(frame)
        if score >= ENTRY_SCORE_THRESHOLD and (best is None or score > best["score"]):
            best = {"symbol": symbol, "score": score, "reasons": reasons}
    return best


def open_orders() -> list:
    assert client is not None
    return list(client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500)))


def submit_buy(symbol: str) -> None:
    assert client is not None
    if not env_bool("ENABLE_LIVE_ORDERS"):
        log.info("Signal-only: live order disabled; candidate=%s", symbol)
        return
    # Re-check the official broker clock immediately before every order. is_open
    # covers regular US market hours and holidays; the order is never queued.
    if not client.get_clock().is_open:
        log.info("Entry cancelled: regular US session is closed.")
        return
    if open_orders():
        log.warning("Entry cancelled: account has an open order already.")
        return
    _, cash = account_snapshot()
    notional = round(min(cash * POSITION_CASH_FRACTION, CAPITAL_CAP_USD * POSITION_CASH_FRACTION), 2)
    if notional < MIN_NOTIONAL_USD:
        log.warning("Entry cancelled: cash available is below minimum notional.")
        return
    order = MarketOrderRequest(symbol=symbol, notional=notional, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
    submitted = client.submit_order(order_data=order)
    state.update(active_symbol=symbol, entry_date=dt.datetime.now(NY).date().isoformat(),
                 entry_price=0.0, peak=0.0, exit_pending=False)
    save_state()
    log.warning("LIVE BUY submitted: %s, notional=$%.2f, order_id=%s", symbol, notional, submitted.id)


def manage_position(position) -> None:
    assert client is not None
    symbol = position.symbol
    if state.get("active_symbol") != symbol:
        log.error("Untracked live position %s detected. Bot will not trade until reconciled.", symbol)
        return
    if state.get("exit_pending"):
        log.info("Waiting for pending live exit on %s.", symbol)
        return
    frame = latest_bars(symbol, days=3)
    if frame.empty:
        log.warning("No fresh bar for %s; exit check skipped.", symbol)
        return
    price = float(frame.iloc[-1]["close"])
    entry = float(position.avg_entry_price)
    peak = max(float(state.get("peak", 0) or price), price)
    state["entry_price"] = entry
    state["peak"] = peak
    save_state()
    pnl = (price - entry) / entry if entry else 0.0
    trail_pct = TRAILING_STOP_AFTER_PROFIT_PCT if pnl >= TRAILING_ACTIVATION_PCT else TRAILING_STOP_PCT
    reason = None
    if price <= entry * (1 - STOP_LOSS_PCT):
        reason = "hard_stop"
    elif price <= peak * (1 - trail_pct):
        reason = "trailing_stop"
    elif pnl >= 0.02 and float(frame.iloc[-1]["return"]) < -0.003:
        reason = "momentum_reversal"
    elif pnl >= TAKE_PROFIT_PCT:
        reason = "take_profit"
    if not reason:
        log.info("Open %s | PnL=%.2f%%", symbol, pnl * 100)
        return
    if not env_bool("ENABLE_LIVE_ORDERS"):
        log.warning("Signal-only exit: %s would trigger %s.", symbol, reason)
        return
    if not client.get_clock().is_open:
        log.warning("Exit signal %s for %s; market is closed, will retry next regular session.", reason, symbol)
        return
    order = MarketOrderRequest(symbol=symbol, qty=str(position.qty), side=OrderSide.SELL, time_in_force=TimeInForce.DAY)
    submitted = client.submit_order(order_data=order)
    state["exit_pending"] = True
    save_state()
    log.warning("LIVE SELL submitted: %s, reason=%s, order_id=%s", symbol, reason, submitted.id)


def run_once() -> None:
    assert client is not None
    update_daily_baseline()
    clock = client.get_clock()
    if not clock.is_open:
        log.info("US regular market session closed; no orders will be submitted.")
        return
    positions = list(client.get_all_positions())
    if len(positions) > 1:
        log.error("More than one live position exists; bot stops trading for safety.")
        return
    if positions:
        manage_position(positions[0])
        return  # Risk exits remain enabled even after the daily loss limit.
    if state.get("active_symbol"):
        if state.get("exit_pending"):
            if state.get("entry_date") == dt.datetime.now(NY).date().isoformat():
                state.setdefault("round_trips", []).append({"date": state["entry_date"], "symbol": state["active_symbol"]})
            state.update(active_symbol=None, entry_date=None, entry_price=0.0, peak=0.0, exit_pending=False)
            save_state()
        else:
            log.info("Waiting for live entry fill/cancel reconciliation.")
            return
    if open_orders():
        log.info("Pending broker order found; no new entry.")
        return
    if daily_loss_hit():
        log.warning("Daily loss cap reached; new entries disabled, open-position exits remain enabled.")
        return
    if recent_round_trip_count() >= MAX_ROUND_TRIPS_IN_5_SESSIONS:
        log.info("Conservative round-trip cap reached (3 within the last 5 Alpaca sessions).")
        return
    candidate = choose_candidate()
    if candidate:
        log.info("Candidate %s score=%s reasons=%s", candidate["symbol"], candidate["score"], candidate["reasons"])
        submit_buy(candidate["symbol"])
    else:
        log.info("No entry signal.")


def main() -> None:
    global client, market_data_client
    load_state()
    client, market_data_client = make_clients()
    equity, cash = account_snapshot()
    log.warning("Connected to Alpaca LIVE account | equity=$%.2f cash=$%.2f cap=$%.2f", equity, cash, CAPITAL_CAP_USD)
    if not env_bool("ENABLE_LIVE_ORDERS"):
        log.warning("Live order switch is off; bot is signal-only.")
    while True:
        try:
            run_once()
        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            return
        except Exception:
            log.exception("Bot cycle failed; no new order was intentionally submitted by this error handler.")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

