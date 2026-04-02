# =========================
# 🚀 PRO AUTO TRADING BOT (FINAL RAILWAY FIX)
# =========================

import time
import threading
import requests
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
RISK_PER_TRADE = 0.02

stocks = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META",
    "TSLA","AMD","PLTR","COIN",
    "SPY","QQQ"
]

ALPACA_API_KEY = "DEIN_API_KEY"
ALPACA_SECRET_KEY = "DEIN_SECRET_KEY"

client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

app = Flask(__name__)

capital = START_CAPITAL
positions = {}

wins = 0
losses = 0


# =========================
# 🛠️ UTILS
# =========================

def safe_float(v):
    try:
        if hasattr(v, "item"):
            v = v.item()
        return float(v)
    except:
        return 0.0


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
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()

    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    return df.dropna()


# =========================
# 🤖 MODEL
# =========================

def train_model(df):
    X = df[["Return","MA20","MA50","RSI"]].values
    y = (df["Return"].shift(-1) > 0).astype(int).values[:-1]
    X = X[:-1]

    scaler = MinMaxScaler()
    X = scaler.fit_transform(X)

    model = Sequential([
        Dense(32, activation='relu'),
        Dense(16, activation='relu'),
        Dense(1, activation='sigmoid')
    ])

    model.compile(optimizer='adam', loss='binary_crossentropy')
    model.fit(X, y, epochs=2, verbose=0)

    return model, scaler


def predict(model, scaler, df):
    latest = df[["Return","MA20","MA50","RSI"]].iloc[-1:].values
    latest = scaler.transform(latest)
    return float(model.predict(latest)[0][0])


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
        print(f"🟢 BUY {symbol} {qty}")
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
        print(f"🔴 SELL {symbol} {qty}")
    except Exception as e:
        print("SELL ERROR:", e)


# =========================
# 🧠 TRADING LOGIC
# =========================

def trade():
    global capital, wins, losses

    for stock in stocks:

        df = get_data(stock)
        if df.empty:
            continue

        model, scaler = train_model(df)
        prob = predict(model, scaler, df)

        price = safe_float(df["Close"].iloc[-1])
        rsi = safe_float(df["RSI"].iloc[-1])
        ma20 = safe_float(df["MA20"].iloc[-1])
        ma50 = safe_float(df["MA50"].iloc[-1])

        volatility = safe_float(df["Return"].rolling(10).std().iloc[-1])
        recent_high = safe_float(df["Close"].rolling(20).max().iloc[-1])

        trend = ma20 > ma50

        print(f"{stock} | prob={prob:.2f} | RSI={rsi:.2f}")

        # ENTRY
        if stock not in positions:

            breakout = price >= recent_high * 0.995

            if (
                prob > 0.6 and
                trend and
                45 < rsi < 60 and
                volatility < 0.04 and
                breakout
            ):

                risk_amount = capital * RISK_PER_TRADE
                qty = int(risk_amount / price)

                if qty > 0:
                    positions[stock] = {
                        "entry": price,
                        "peak": price,
                        "qty": qty
                    }

                    capital -= qty * price
                    buy(stock, qty)

        # EXIT
        pos = positions.get(stock)

        if pos:

            entry = pos["entry"]
            peak = pos["peak"]
            qty = pos["qty"]

            if price > peak:
                pos["peak"] = price

            tp = entry * (1.02 + volatility)
            sl = entry * (0.99 - volatility * 0.5)
            trailing = peak * 0.98

            if (
                price >= tp or
                price <= sl or
                price <= trailing or
                rsi > 70 or
                prob < 0.45
            ):
                capital += qty * price
                profit = (price - entry) * qty

                if profit > 0:
                    wins += 1
                else:
                    losses += 1

                sell(stock, qty)

                print(f"🔴 SELL {stock} Profit: {profit:.2f}")

                del positions[stock]


# =========================
# 🔁 AUTO LOOP
# =========================

def auto_trade_loop():
    print("🤖 Auto-Trading gestartet...")

    while True:
        try:
            trade()
        except Exception as e:
            print("❌ Fehler:", e)

        time.sleep(60)


# =========================
# 🚀 RAILWAY AUTO START FIX
# =========================

bot_started = False

@app.before_request
def start_bot_once():
    global bot_started

    if not bot_started:
        print("🚀 Starte Bot jetzt (Railway Fix)...")

        thread = threading.Thread(target=auto_trade_loop)
        thread.daemon = True
        thread.start()

        bot_started = True


# =========================
# 🌐 DASHBOARD
# =========================

@app.route("/")
def dashboard():
    return f"<h1>💰 Capital: {capital:.2f}</h1><h2>{positions}</h2>"


# =========================
# ▶️ START (lokal)
# =========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)