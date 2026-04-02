# =========================
# 🚀 PRO BOT (COMPOUNDING + BALANCED MODE)
# =========================

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

stocks = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD"]

ALPACA_API_KEY = "DEIN_API_KEY"
ALPACA_SECRET_KEY = "DEIN_SECRET_KEY"

client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

app = Flask(__name__)

capital = START_CAPITAL
peak_capital = START_CAPITAL

positions = {}

wins = 0
losses = 0
total_trades = 0

equity_history = []


# =========================
# 🧠 MODEL CACHE
# =========================

model_cache = {}
last_train_time = {}
TRAIN_INTERVAL = 900


# =========================
# 🛠️ UTILS
# =========================

def safe_float(v):
    try:
        return float(v.item() if hasattr(v, "item") else v)
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
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()

    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    return df.dropna()


# =========================
# 🧠 AI SCORE
# =========================

def calculate_score(prob, trend, rsi, vol):
    score = prob

    if trend:
        score += 0.1

    if 40 < rsi < 65:  # 🔥 lockerer
        score += 0.05

    if vol > 0.002:  # 🔥 lockerer
        score += 0.05

    return score


# =========================
# 💰 COMPOUNDING RISK
# =========================

def dynamic_risk():
    global capital, peak_capital

    if capital > peak_capital:
        peak_capital = capital
        return BASE_RISK * 1.5

    drawdown = (peak_capital - capital) / peak_capital

    if drawdown > 0.1:
        return BASE_RISK * 0.5

    return BASE_RISK


def get_position_size(score):
    risk = dynamic_risk()

    if score > 0.75:
        risk *= 1.5
    elif score > 0.65:
        risk *= 1.2

    return capital * risk


# =========================
# 🤖 MODEL
# =========================

def get_or_train_model(symbol, df):
    global model_cache, last_train_time

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
    global capital, wins, losses, total_trades, equity_history

    for stock in stocks:

        df = get_data(stock)
        if df.empty:
            continue

        model, scaler = get_or_train_model(stock, df)
        prob = predict(model, scaler, df)

        price = safe_float(df["Close"].iloc[-1])
        rsi = safe_float(df["RSI"].iloc[-1])
        ma20 = safe_float(df["MA20"].iloc[-1])
        ma50 = safe_float(df["MA50"].iloc[-1])

        vol = safe_float(df["Return"].rolling(10).std().iloc[-1])
        recent_high = safe_float(df["Close"].rolling(20).max().iloc[-1])

        trend = ma20 > ma50 and price > ma20

        score = calculate_score(prob, trend, rsi, vol)

        print(f"{stock} | score={score:.2f}")

        # =========================
        # ENTRY (AGGRESSIVER)
        # =========================
        if stock not in positions:

            breakout = price >= recent_high * 0.985  # 🔥 lockerer

            if score > 0.60 and breakout:  # 🔥 lockerer

                risk_amount = get_position_size(score)
                qty = int(risk_amount / price)

                if qty > 0:
                    positions[stock] = {
                        "entry": price,
                        "peak": price,
                        "qty": qty
                    }

                    capital -= qty * price
                    buy(stock, qty)

        # =========================
        # EXIT
        # =========================
        pos = positions.get(stock)

        if pos:

            entry = pos["entry"]
            peak = pos["peak"]
            qty = pos["qty"]

            if price > peak:
                pos["peak"] = price

            trailing = peak * 0.96

            if price <= trailing or score < 0.5:

                capital += qty * price
                profit = (price - entry) * qty

                total_trades += 1

                if profit > 0:
                    wins += 1
                else:
                    losses += 1

                sell(stock, qty)

                print(f"💰 Profit: {profit:.2f}")

                del positions[stock]

    equity_history.append(capital)


# =========================
# 🔁 LOOP
# =========================

def auto_trade_loop():
    print("🤖 Trading gestartet...")

    while True:
        trade()
        time.sleep(60)


threading.Thread(target=auto_trade_loop, daemon=True).start()


# =========================
# 📊 DASHBOARD
# =========================

@app.route("/")
def dashboard():
    winrate = (wins / total_trades * 100) if total_trades > 0 else 0
    chart_data = ",".join([str(x) for x in equity_history[-100:]])

    return f"""
    <html>
    <head>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>

    <body style="background:black;color:white">

    <h1>💰 Capital: {capital:.2f}</h1>
    <h2>Trades: {total_trades}</h2>
    <h2>Winrate: {winrate:.2f}%</h2>

    <canvas id="chart"></canvas>

    <script>
    new Chart(document.getElementById('chart'), {{
        type: 'line',
        data: {{
            labels: [...Array({len(equity_history[-100:])}).keys()],
            datasets: [{{
                label: 'Equity',
                data: [{chart_data}],
                borderColor: 'yellow'
            }}]
        }}
    }});
    </script>

    </body>
    </html>
    """