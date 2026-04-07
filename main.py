# =========================
# 🚀 REAL MONEY BOT (50€ OPTIMIZED FINAL)
# =========================

import os
import time
import threading
import requests
import yfinance as yf

from flask import Flask
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce


# =========================
# ⚙️ CONFIG (🔥 50€ MODE)
# =========================

BASE_RISK = 0.005  # 🔥 nur 0.5% pro Trade
MAX_POSITIONS = 2
MAX_DRAWDOWN = 0.10
MAX_TRADES_PER_HOUR = 10

# 🔥 ERWEITERTE STOCK LISTE (diversifiziert)
stocks = [
    "AAPL","MSFT","NVDA","TSLA","AMD",
    "META","AMZN","GOOGL",
    "SPY","QQQ",
    "PLTR","COIN"
]

ALPACA_API_KEY = "AKDDE6KSWJL7KTCXDG6EDQV4NA"
ALPACA_SECRET_KEY = "6RAewbvzy9SNJfhg8G2eeV9BEfpJxUqGzfQbKG4rtj2B"

TELEGRAM_TOKEN = "8632163884:AAFBAX81sywEZxp4raQFXUsy2SY60ZCA378"
TELEGRAM_CHAT_ID = "7797525649"

client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=False)

app = Flask(__name__)

positions = {}
trade_timestamps = []
bot_active = True
peak_equity = 0


# =========================
# 📲 TELEGRAM
# =========================

def send_msg(text):
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text}
        )
    except:
        pass


# =========================
# 💰 ACCOUNT
# =========================

def get_equity():
    acc = client.get_account()
    return float(acc.equity)


# =========================
# 🛠️ SAFE
# =========================

def get_last(v):
    try:
        if hasattr(v, "iloc"):
            v = v.iloc[-1]
        return float(v)
    except:
        return 0.0


# =========================
# 📊 DATA
# =========================

def get_data(symbol):
    df = yf.download(symbol, period="2d", interval="5m")

    if df.empty:
        return df

    df["Return"] = df["Close"].pct_change()
    df["MA20"] = df["Close"].rolling(20).mean()

    return df.dropna()


# =========================
# 🧠 SIGNAL (STABIL)
# =========================

def signal(df):
    price = get_last(df["Close"])
    ma20 = get_last(df["MA20"])
    momentum = get_last(df["Return"])

    score = 0

    if price > ma20:
        score += 1

    if momentum > 0.001:
        score += 1

    if price < ma20 * 1.01:
        score += 1

    return score


# =========================
# 🛑 RISK CONTROL
# =========================

def risk_check():
    global peak_equity, bot_active

    equity = get_equity()

    if equity > peak_equity:
        peak_equity = equity

    dd = (peak_equity - equity) / peak_equity if peak_equity else 0

    if dd > MAX_DRAWDOWN:
        bot_active = False
        send_msg(f"🛑 BOT STOPPED (Drawdown {dd:.2%})")


def can_trade():
    now = time.time()
    global trade_timestamps

    trade_timestamps = [t for t in trade_timestamps if now - t < 3600]

    return len(trade_timestamps) < MAX_TRADES_PER_HOUR


# =========================
# 💰 EXECUTION
# =========================

def buy(symbol, notional):
    try:
        client.submit_order(MarketOrderRequest(
            symbol=symbol,
            notional=notional,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        ))

        trade_timestamps.append(time.time())
        send_msg(f"🟢 BUY {symbol} ${notional:.2f}")

    except Exception as e:
        send_msg(f"❌ BUY ERROR: {e}")


def sell(symbol):
    try:
        pos = client.get_open_position(symbol)

        client.submit_order(MarketOrderRequest(
            symbol=symbol,
            qty=pos.qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        ))

        send_msg(f"🔴 SELL {symbol}")

    except Exception as e:
        send_msg(f"❌ SELL ERROR: {e}")


# =========================
# 🚀 LOGIC
# =========================

def trade():
    global positions

    if not bot_active:
        return

    risk_check()

    if not can_trade():
        return

    equity = get_equity()

    # 🔥 MINIMUM CHECK (wichtig bei 50€)
    if equity < 10:
        send_msg("⚠️ Zu wenig Kapital")
        return

    for stock in stocks:

        df = get_data(stock)
        if df.empty:
            continue

        score = signal(df)
        price = get_last(df["Close"])

        print(f"{stock} score={score}")

        # ENTRY
        if stock not in positions and len(positions) < MAX_POSITIONS:

            if score >= 2:

                size = max(equity * BASE_RISK, 2)  # 🔥 min $2

                buy(stock, size)

                positions[stock] = {
                    "entry": price,
                    "peak": price
                }

        # EXIT
        elif stock in positions:

            pos = positions[stock]

            if price > pos["peak"]:
                pos["peak"] = price

            trailing = pos["peak"] * 0.97

            if price <= trailing:

                sell(stock)
                del positions[stock]


# =========================
# 🔁 LOOP
# =========================

def loop():
    send_msg("🚀 BOT LIVE (50€ MODE)")

    while True:
        try:
            trade()
        except Exception as e:
            send_msg(f"❌ ERROR: {e}")

        time.sleep(60)


threading.Thread(target=loop, daemon=True).start()


# =========================
# 🌐 DASHBOARD
# =========================

@app.route("/")
def dashboard():
    return f"<h1>💰 BOT RUNNING (50€ MODE)</h1>"