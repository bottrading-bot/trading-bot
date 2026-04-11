import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf

from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM
from tensorflow.keras import Input

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# =========================
# CONFIG
# =========================
START_CAPITAL = 50
SLEEP_TIME = 120

MAX_POSITIONS = 2   # 🔥 jetzt 2 Trades möglich
MAX_DAILY_LOSS = 0.05
MAX_DRAWDOWN = 0.15

stocks = [
    "AAPL","MSFT","TSLA","AMZN","NVDA",
    "META","GOOGL","AMD","SPY","QQQ"
]

TOKEN = "8632163884:AAFBAX81sywEZxp4raQFXUsy2SY60ZCA378"
CHAT_ID = "7797525649"

ALPACA_API_KEY = "AKDDE6KSWJL7KTCXDG6EDQV4NA"
ALPACA_SECRET_KEY = "6RAewbvZy9SNJfhg862eeV9BEfpJxUq6zfQbK64rtj2B"

client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=False)

# =========================
# HELPERS
# =========================
def send_message(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        pass

def safe_float(x):
    try:
        if hasattr(x, "item"):
            x = x.item()
        return float(x)
    except:
        return 0.0

# =========================
# ALPACA
# =========================
def execute_buy(symbol, qty):
    try:
        order = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        )
        client.submit_order(order)
        print(f"🟢 BUY {symbol} {qty}")
    except Exception as e:
        print("BUY ERROR:", e)

def execute_sell(symbol, qty):
    try:
        order = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        )
        client.submit_order(order)
        print(f"🔴 SELL {symbol} {qty}")
    except Exception as e:
        print("SELL ERROR:", e)

# =========================
# DATA
# =========================
def get_data(symbol):
    df = yf.download(symbol, period="7d", interval="5m")

    if df.empty:
        return df

    df["Return"] = df["Close"].pct_change()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA50"] = df["Close"].rolling(50).mean()

    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()

    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    return df.dropna()

# =========================
# MARKET
# =========================
def market_regime(df):
    ma20 = safe_float(df["MA20"].iloc[-1])
    ma50 = safe_float(df["MA50"].iloc[-1])
    return "BULL" if ma20 > ma50 else "BEAR"

# =========================
# MODEL
# =========================
def train_model(df):
    features = df[["Return","MA20","MA50","RSI"]].values

    X, y = [], []
    for i in range(20, len(features)-1):
        X.append(features[i-20:i])
        y.append(1 if features[i+1][0] > 0 else 0)

    X = np.array(X)
    y = np.array(y)

    scaler = MinMaxScaler()
    X = scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape)

    model = Sequential([
        Input(shape=(X.shape[1], X.shape[2])),
        LSTM(32, return_sequences=True),
        LSTM(16),
        Dense(1, activation="sigmoid")
    ])

    model.compile(optimizer="adam", loss="binary_crossentropy")
    model.fit(X, y, epochs=3, verbose=0)

    return model, scaler

def predict(model, scaler, df):
    features = df[["Return","MA20","MA50","RSI"]].values[-20:]
    features = scaler.transform(features)
    features = np.array([features])
    return float(model.predict(features)[0][0])

# =========================
# 🔥 DYNAMISCHE POSITION SIZE
# =========================
def position_size(capital, vol, pred, regime):

    base = 0.4

    if pred > 0.8:
        base += 0.1   # 🔥 mehr Einsatz bei starker KI

    if regime == "BULL":
        base += 0.05

    if vol > 0.03:
        base *= 0.5

    return capital * base

def volatility_filter(vol):
    return vol < 0.05

# =========================
# MAIN
# =========================
capital = START_CAPITAL
positions = {}
peak_capital = capital
daily_start = capital
profits = []

try:
    while True:
        print("\n===== NEXT LEVEL BOT =====")
        print(f"💰 Kapital: {capital:.2f}")

        if (capital - daily_start) / daily_start < -MAX_DAILY_LOSS:
            send_message("🛑 STOP Tagesverlust")
            break

        if capital > peak_capital:
            peak_capital = capital

        if (peak_capital - capital) / peak_capital > MAX_DRAWDOWN:
            send_message("🛑 STOP Drawdown")
            break

        for stock in stocks:

            print("POSITIONEN:", positions)

            # SELL darf IMMER laufen
            if stock not in positions and len(positions) >= MAX_POSITIONS:
                continue

            df = get_data(stock)
            if df.empty or len(df) < 50:
                continue

            regime = market_regime(df)

            if stock not in models:
                model, scaler = train_model(df)
                models[stock] = model
                scalers[stock] = scaler

            model = models[stock]
            scaler = scalers[stock]

            pred = predict(model, scaler, df)

            price = safe_float(df["Close"].iloc[-1])
            rsi = safe_float(df["RSI"].iloc[-1])
            ma20 = safe_float(df["MA20"].iloc[-1])
            ma50 = safe_float(df["MA50"].iloc[-1])
            vol = safe_float(df["Return"].rolling(10).std().iloc[-1])
            momentum = df["Close"].pct_change(3).iloc[-1]

            trend = ma20 > ma50

            print(f"{stock} | {price:.2f} | AI:{pred:.2f} | {regime}")

            # ================= BUY =================
            # ================= BUY =================
            if stock not in positions:

                if (
                        regime == "BULL"
                        and pred > 0.52  # 🔥 nicht zu streng
                        and trend
                        and 35 < rsi < 65  # 🔥 breiter Bereich
                        and volatility_filter(vol)
                        and price > ma20  # Trend bestätigt
                ):
                    amount = position_size(capital, vol, pred, regime)
                    qty = max(1, int(amount / price))

                    if qty > 0:
                        positions[stock] = {
                            "entry": price,
                            "peak": price,
                            "qty": qty
                        }

                        capital -= qty * price
                        execute_buy(stock, qty)
                        send_message(f"🟢 BUY {stock} @ {price}")
            # ================= SELL =================
            elif stock in positions:

                pos = positions[stock]
                entry = pos["entry"]
                peak = pos["peak"]
                qty = pos["qty"]

                if price > peak:
                    pos["peak"] = price
                    peak = price

                tp = 1.01 + vol
                sl = 0.98
                trailing = peak * 0.997

                if price > entry * 1.02:
                    capital += qty * price
                    execute_sell(stock, qty)
                    send_message(f"💰 QUICK PROFIT {stock}")
                    del positions[stock]
                    continue

                if price > entry * 1.008 and price < peak:
                    capital += qty * price
                    execute_sell(stock, qty)
                    send_message(f"🔒 PROFIT LOCK {stock}")
                    del positions[stock]
                    continue

                if (
                    price >= entry * tp
                    or price <= entry * sl
                    or price <= trailing
                    or rsi > 75
                    or pred < 0.45
                ):
                    capital += qty * price
                    profit = (price - entry) * qty
                    profits.append(profit)

                    execute_sell(stock, qty)
                    send_message(f"🔴 SELL {stock} Gewinn: {round(profit,2)}")

                    del positions[stock]

        if profits:
            total = sum(profits)
            winrate = len([p for p in profits if p > 0]) / len(profits) * 100
            print(f"📊 Profit: {round(total,2)} | Winrate: {round(winrate,2)}%")

        time.sleep(SLEEP_TIME)

except KeyboardInterrupt:
    print("🛑 Bot gestoppt")