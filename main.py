# =========================
# 🚀 ELITE MULTI-STRATEGY BOT
# =========================

import os
import time
import threading
import yfinance as yf
import numpy as np

from flask import Flask
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce


# =========================
# ⚙️ CONFIG
# =========================

START_CAPITAL = 100000
BASE_RISK = 0.02
MAX_DRAWDOWN = 0.15

stocks = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","PLTR"]

ALPACA_API_KEY = "PKITPHJQN7QSKR5NIIVXAQAS2C"
ALPACA_SECRET_KEY = "AQUNyqwMS95CDsUEKfYsCmCwjag84ZfNDHjpTCsqdRWm"

client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

app = Flask(__name__)

capital = START_CAPITAL
peak_capital = START_CAPITAL

positions = {}
equity_history = []

wins = 0
losses = 0
total_trades = 0


# =========================
# 🧠 MODEL CACHE
# =========================

model_cache = {}
last_train_time = {}
TRAIN_INTERVAL = 900


# =========================
# 📊 DATA
# =========================

def get_data(symbol):
    df = yf.download(symbol, period="5d", interval="5m")

    if df.empty:
        return df

    df["Return"] = df["Close"].pct_change()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA50"] = df["Close"].rolling(50).mean()

    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()

    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    return df.dropna()


# =========================
# 🧠 MARKET REGIME
# =========================

def market_regime():
    df = get_data("SPY")
    if df.empty:
        return "neutral"

    ma20 = df["MA20"].iloc[-1]
    ma50 = df["MA50"].iloc[-1]

    if ma20 > ma50:
        return "bull"
    elif ma20 < ma50:
        return "bear"
    return "neutral"


# =========================
# 🧠 AI MODEL
# =========================

def get_model(symbol, df):
    now = time.time()

    if symbol not in model_cache or now - last_train_time.get(symbol, 0) > TRAIN_INTERVAL:

        X = df[["Return","MA20","MA50","RSI"]].values
        y = (df["Return"].shift(-1) > 0).astype(int).values[:-1]
        X = X[:-1]

        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(X)

        model = Sequential([
            Dense(64, activation='relu'),
            Dense(32, activation='relu'),
            Dense(1, activation='sigmoid')
        ])

        model.compile(optimizer='adam', loss='binary_crossentropy')
        model.fit(X_scaled, y, epochs=3, verbose=0)

        model_cache[symbol] = (model, scaler)
        last_train_time[symbol] = now

    return model_cache[symbol]


def predict(model, scaler, df):
    latest = df[["Return","MA20","MA50","RSI"]].iloc[-1:].values
    latest = scaler.transform(latest)
    return float(model.predict(latest)[0][0])


# =========================
# 💰 RISK ENGINE
# =========================

def dynamic_risk():
    global capital, peak_capital

    if capital > peak_capital:
        peak_capital = capital
        return BASE_RISK * 1.5

    dd = (peak_capital - capital) / peak_capital

    if dd > MAX_DRAWDOWN:
        return BASE_RISK * 0.3
    elif dd > 0.08:
        return BASE_RISK * 0.6

    return BASE_RISK


def position_size(score):
    risk = dynamic_risk()

    if score > 0.8:
        risk *= 2
    elif score > 0.7:
        risk *= 1.5

    return capital * risk


# =========================
# 🧠 STRATEGIES
# =========================

def strategy(df):
    price = df["Close"].iloc[-1]
    ma20 = df["MA20"].iloc[-1]
    ma50 = df["MA50"].iloc[-1]
    rsi = df["RSI"].iloc[-1]

    recent_high = df["Close"].rolling(20).max().iloc[-1]

    trend = ma20 > ma50 and price > ma20
    breakout = price >= recent_high * 0.985
    pullback = price <= ma20 * 0.99

    score = 0

    if trend:
        score += 0.3
    if breakout:
        score += 0.3
    if pullback:
        score += 0.2
    if 40 < rsi < 65:
        score += 0.2

    return score


# =========================
# 💰 EXECUTION
# =========================

def buy(symbol, qty):
    try:
        client.submit_order(MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        ))
        print(f"BUY {symbol}")
    except Exception as e:
        print("BUY ERROR:", e)


def sell(symbol, qty):
    try:
        client.submit_order(MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        ))
        print(f"SELL {symbol}")
    except Exception as e:
        print("SELL ERROR:", e)


# =========================
# 🚀 MAIN LOGIC
# =========================

def trade():
    global capital, wins, losses, total_trades

    regime = market_regime()

    for stock in stocks:

        df = get_data(stock)
        if df.empty:
            continue

        model, scaler = get_model(stock, df)
        prob = predict(model, scaler, df)

        strat_score = strategy(df)
        score = prob + strat_score

        price = df["Close"].iloc[-1]

        if stock not in positions:

            if regime == "bear":
                continue

            if score > 1.2:
                size = position_size(score)
                qty = int(size / price)

                if qty > 0:
                    positions[stock] = {
                        "entry": price,
                        "peak": price,
                        "qty": qty
                    }

                    capital -= qty * price
                    buy(stock, qty)

        else:
            pos = positions[stock]

            if price > pos["peak"]:
                pos["peak"] = price

            trailing = pos["peak"] * 0.94

            if price <= trailing or score < 0.8:
                capital += pos["qty"] * price

                profit = (price - pos["entry"]) * pos["qty"]

                total_trades += 1
                if profit > 0:
                    wins += 1
                else:
                    losses += 1

                sell(stock, pos["qty"])
                del positions[stock]

    equity_history.append(capital)


# =========================
# 🔁 LOOP
# =========================

def loop():
    while True:
        trade()
        time.sleep(60)


threading.Thread(target=loop, daemon=True).start()


# =========================
# 📊 DASHBOARD
# =========================

@app.route("/")
def dashboard():
    winrate = (wins / total_trades * 100) if total_trades else 0

    return f"""
    <h1>Capital: {capital:.2f}</h1>
    <h2>Winrate: {winrate:.2f}%</h2>
    <h2>Trades: {total_trades}</h2>
    <h2>Positions: {positions}</h2>
    """