# =========================
# 🚀 ELITE BOT (MULTI POSITION MODE)
# =========================

import os
import time
import threading
import yfinance as yf

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
MAX_POSITIONS = 5  # 🔥 NEU

stocks = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","PLTR"]

ALPACA_API_KEY = "PK7DOZIWI3YVQZEGSEJQJREQ4U"
ALPACA_SECRET_KEY = "8hEWZL748aiZzMrGVYALDt2VzXGxqxhr6AYEYkf7qYD2"

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise ValueError("❌ API Keys fehlen!")

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
# 🛠️ SAFE DATA ACCESS
# =========================

def get_last(df, col):
    try:
        val = df[col]

        # Wenn Series → letzten Wert nehmen
        if hasattr(val, "iloc"):
            val = val.iloc[-1]

        # numpy → float
        if hasattr(val, "item"):
            val = val.item()

        return float(val)

    except Exception as e:
        print(f"❌ get_last Fehler bei {col}: {e}")
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
# 🧠 STRATEGY
# =========================

def strategy(df):
    price = get_last(df, "Close")
    ma20 = get_last(df, "MA20")
    ma50 = get_last(df, "MA50")
    rsi = get_last(df, "RSI")

    recent_high = get_last(
        df.assign(high=df["Close"].rolling(20).max()),
        "high"
    )

    trend = ma20 > ma50 and price > ma20
    breakout = price >= recent_high * 0.985
    pullback = price <= ma20 * 0.99

    score = 0.0

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
# 🧠 MODEL
# =========================

model_cache = {}
last_train_time = {}
TRAIN_INTERVAL = 900


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
    elif score > 0.6:
        risk *= 1.3

    return capital * risk


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
# 🚀 TRADING LOGIC
# =========================

def trade():
    global capital, wins, losses, total_trades, equity_history

    for stock in stocks:

        df = get_data(stock)
        if df.empty:
            continue

        model, scaler = get_model(stock, df)
        prob = predict(model, scaler, df)

        strat_score = strategy(df)
        score = prob + strat_score

        price = get_last(df, "Close")

        print(f"{stock} | score={score:.2f}")

        # =========================
        # ENTRY (FIXED + AKTIV)
        # =========================
        if stock not in positions and len(positions) < MAX_POSITIONS:

            # 🔥 richtiger breakout (WICHTIG)
            recent_high = float(df["Close"].rolling(20).max().iloc[-1])
            breakout = price >= recent_high * 0.985

            # 🔥 flexibler entry (mehr trades)
            if (score > 0.9 or prob > 0.6) and breakout:
                available_capital = capital / (MAX_POSITIONS - len(positions) + 1)
                size = min(position_size(score), available_capital)

                qty = int(size / price)

        # =========================
        # EXIT
        # =========================
        elif stock in positions:

            pos = positions[stock]

            if price > pos["peak"]:
                pos["peak"] = price

            trailing = pos["peak"] * 0.93

            if price <= trailing or score < 0.75:

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
    print("🤖 MULTI BOT läuft...")

    while True:
        try:
            trade()
        except Exception as e:
            print("❌ Fehler:", e)

        time.sleep(60)


threading.Thread(target=loop, daemon=True).start()


# =========================
# 📊 DASHBOARD
# =========================

@app.route("/")
def dashboard():
    winrate = (wins / total_trades * 100) if total_trades else 0

    return f"""
    <h1>💰 Capital: {capital:.2f}</h1>
    <h2>Trades: {total_trades}</h2>
    <h2>Winrate: {winrate:.2f}%</h2>
    <h2>Open Positions: {len(positions)}</h2>
    <h3>{positions}</h3>
    """