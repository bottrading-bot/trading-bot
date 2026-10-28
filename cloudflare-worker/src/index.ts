interface Env {
  ALPACA_API_KEY: string;
  ALPACA_SECRET_KEY: string;
  ENABLE_LIVE_ORDERS?: string;
  CAPITAL_CAP_USD?: string;
  MAX_TRADE_LOSS_USD?: string;
  BOT_STATE: KVNamespace;
}

interface Bar {
  t: string;
  c: number;
  v: number;
}

interface TradeState {
  sessionDate: string | null;
  dayStartEquity: number;
  active: { symbol: string; entryOrderId: string; clientOrderId: string } | null;
  roundTrips: Array<{ date: string; symbol: string }>;
}

interface AlpacaOrder {
  id: string;
  client_order_id?: string;
  symbol?: string;
  status: string;
  legs?: AlpacaOrder[] | null;
  side?: string;
  type?: string;
  order_class?: string;
}

const API = "https://api.alpaca.markets";
const DATA = "https://data.alpaca.markets";
const NY_ZONE = "America/New_York";
const WATCHLIST = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "PLTR", "SNOW", "COIN", "RIOT", "MARA", "SPY", "QQQ"];
const STOP_LOSS_PCT = 0.03;
const TAKE_PROFIT_PCT = 0.05;
const POSITION_CASH_FRACTION = 0.90;
const MAX_DAILY_LOSS_PCT = 0.05;
const MAX_ROUND_TRIPS_IN_5_SESSIONS = 3;
const ENTRY_SCORE_THRESHOLD = 4;
const STATE_KEY = "trading-state-v1";

const EMPTY_STATE: TradeState = {
  sessionDate: null,
  dayStartEquity: 0,
  active: null,
  roundTrips: [],
};

function log(message: string, detail?: unknown): void {
  if (detail === undefined) console.log(message);
  else console.log(message, detail);
}

function nyParts(date: Date): Record<string, string> {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: NY_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  return Object.fromEntries(parts.map((part) => [part.type, part.value]));
}

function nyDate(date = new Date()): string {
  const p = nyParts(date);
  return `${p.year}-${p.month}-${p.day}`;
}

function isRegularSessionBar(timestamp: string): boolean {
  const p = nyParts(new Date(timestamp));
  const weekday = !["Sat", "Sun"].includes(p.weekday);
  const minute = Number(p.hour) * 60 + Number(p.minute);
  return weekday && minute >= 9 * 60 + 30 && minute < 16 * 60;
}

function isCompletedBar(timestamp: string, now: Date): boolean {
  const cutoff = new Date(Math.floor(now.getTime() / 300_000) * 300_000);
  return new Date(timestamp).getTime() < cutoff.getTime();
}

function numberEnv(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

async function alpaca<T>(env: Env, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("APCA-API-KEY-ID", env.ALPACA_API_KEY);
  headers.set("APCA-API-SECRET-KEY", env.ALPACA_SECRET_KEY);
  if (init.body) headers.set("Content-Type", "application/json");
  const response = await fetch(`${API}${path}`, { ...init, headers });
  const text = await response.text();
  if (!response.ok) throw new Error(`Alpaca ${init.method ?? "GET"} ${path}: ${response.status} ${text.slice(0, 500)}`);
  return text ? JSON.parse(text) as T : ({} as T);
}

async function getState(env: Env): Promise<TradeState> {
  const saved = await env.BOT_STATE.get<TradeState>(STATE_KEY, "json");
  if (!saved) return structuredClone(EMPTY_STATE);
  if (!Array.isArray(saved.roundTrips) || !("active" in saved)) {
    throw new Error("Stored bot state is invalid; refusing to trade.");
  }
  return { ...EMPTY_STATE, ...saved };
}

async function putState(env: Env, state: TradeState): Promise<void> {
  await env.BOT_STATE.put(STATE_KEY, JSON.stringify(state));
}

async function getBars(env: Env, now: Date): Promise<Map<string, Bar[]>> {
  const params = new URLSearchParams({
    symbols: WATCHLIST.join(","),
    timeframe: "5Min",
    start: new Date(now.getTime() - 5 * 24 * 60 * 60 * 1000).toISOString(),
    end: now.toISOString(),
    limit: "10000",
    feed: "iex",
    sort: "asc",
  });
  const allRows: Record<string, Bar[]> = {};
  let pageToken: string | null = null;
  do {
    if (pageToken) params.set("page_token", pageToken);
    const response = await fetch(`${DATA}/v2/stocks/bars?${params}`, {
      headers: { "APCA-API-KEY-ID": env.ALPACA_API_KEY, "APCA-API-SECRET-KEY": env.ALPACA_SECRET_KEY },
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`Alpaca market data: ${response.status} ${text.slice(0, 500)}`);
    const payload = JSON.parse(text) as { bars?: Record<string, Bar[]>; next_page_token?: string | null };
    for (const [symbol, rows] of Object.entries(payload.bars ?? {})) {
      allRows[symbol] ??= [];
      allRows[symbol].push(...rows);
    }
    pageToken = payload.next_page_token ?? null;
  } while (pageToken);
  const result = new Map<string, Bar[]>();
  for (const [symbol, rows] of Object.entries(allRows)) {
    result.set(symbol, rows
      .filter((bar) => isRegularSessionBar(bar.t) && isCompletedBar(bar.t, now))
      .map((bar) => ({ ...bar, c: Number(bar.c), v: Number(bar.v) }))
      .filter((bar) => Number.isFinite(bar.c) && Number.isFinite(bar.v))
      .sort((a, b) => a.t.localeCompare(b.t)));
  }
  return result;
}

function mean(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function indicators(bars: Bar[]): Array<{ close: number; volume: number; ret: number; ma20: number; ma50: number; ema9: number; rsi14: number; vol20: number }> {
  if (bars.length < 60) return [];
  const closes = bars.map((bar) => bar.c);
  const volumes = bars.map((bar) => bar.v);
  const ema: number[] = [];
  const alpha = 2 / 10;
  for (let i = 0; i < closes.length; i++) {
    ema.push(i === 0 ? closes[i] : alpha * closes[i] + (1 - alpha) * ema[i - 1]);
  }
  const result = [];
  for (let i = 50; i < bars.length; i++) {
    const changes = [];
    for (let j = i - 13; j <= i; j++) changes.push(closes[j] - closes[j - 1]);
    const gains = changes.map((change) => Math.max(change, 0));
    const losses = changes.map((change) => Math.max(-change, 0));
    const avgGain = mean(gains);
    const avgLoss = mean(losses);
    const rsi = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
    result.push({
      close: closes[i],
      volume: volumes[i],
      ret: closes[i] / closes[i - 1] - 1,
      ma20: mean(closes.slice(i - 19, i + 1)),
      ma50: mean(closes.slice(i - 49, i + 1)),
      ema9: ema[i],
      rsi14: rsi,
      vol20: mean(volumes.slice(i - 19, i + 1)),
    });
  }
  return result;
}

function scoreBars(bars: Bar[]): { score: number; reasons: string[]; price: number } | null {
  const rows = indicators(bars);
  if (rows.length < 2) return null;
  const current = rows[rows.length - 1];
  const previous = rows[rows.length - 2];
  const checks: Array<[boolean, string]> = [
    [current.ma20 > current.ma50, "trend"],
    [current.ema9 > current.ma20, "ema"],
    [current.ret > 0.002 && previous.ret > 0, "momentum"],
    [current.rsi14 >= 48 && current.rsi14 <= 72, "rsi"],
    [current.volume > current.vol20, "volume"],
  ];
  const reasons = checks.filter(([passed]) => passed).map(([, name]) => name);
  return { score: reasons.length, reasons, price: current.close };
}

async function recentRoundTrips(env: Env, state: TradeState, today: string): Promise<number> {
  const from = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const url = new URL(`${API}/v2/calendar`);
  url.searchParams.set("start", from);
  url.searchParams.set("end", today);
  const response = await fetch(url, {
    headers: { "APCA-API-KEY-ID": env.ALPACA_API_KEY, "APCA-API-SECRET-KEY": env.ALPACA_SECRET_KEY },
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Alpaca calendar: ${response.status} ${text.slice(0, 400)}`);
  const sessions = (JSON.parse(text) as Array<{ date: string }>).map((row) => row.date).slice(-5);
  const window = new Set(sessions);
  state.roundTrips = state.roundTrips.filter((trip) => window.has(trip.date));
  await putState(env, state);
  return state.roundTrips.length;
}

async function loadOrders(env: Env): Promise<AlpacaOrder[]> {
  return alpaca<AlpacaOrder[]>(env, "/v2/orders?status=open&limit=500&nested=true");
}

async function reconcileClosedEntry(env: Env, state: TradeState, today: string): Promise<boolean> {
  const active = state.active;
  if (!active) return true;
  const positionUrl = `${API}/v2/positions/${encodeURIComponent(active.symbol)}`;
  const positionResponse = await fetch(positionUrl, {
    headers: { "APCA-API-KEY-ID": env.ALPACA_API_KEY, "APCA-API-SECRET-KEY": env.ALPACA_SECRET_KEY },
  });
  if (positionResponse.ok) return false;
  if (positionResponse.status !== 404) throw new Error(`Alpaca position lookup failed: ${positionResponse.status}`);

  const order = await alpaca<AlpacaOrder>(env, `/v2/orders/${encodeURIComponent(active.entryOrderId)}?nested=true`);
  const orderStatus = order.status.toLowerCase();
  if (["new", "accepted", "pending_new", "partially_filled", "pending_cancel", "pending_replace"].includes(orderStatus)) return false;
  if (["canceled", "rejected", "expired"].includes(orderStatus)) {
    log(`Entry order ended as ${orderStatus}; clearing pending bot state.`);
    state.active = null;
    await putState(env, state);
    return true;
  }
  const stopOrTargetFilled = (order.legs ?? []).some((leg) => leg.side === "sell" && leg.status.toLowerCase() === "filled");
  if (orderStatus === "filled" && stopOrTargetFilled) {
    state.roundTrips.push({ date: today, symbol: active.symbol });
    log(`Closed bot trade recorded for ${today}: ${active.symbol}`);
    state.active = null;
    await putState(env, state);
    return true;
  }
  log(`Entry ${active.entryOrderId} is not reconciled yet; blocking new trades.`);
  return false;
}

async function run(env: Env): Promise<void> {
  if (!env.ALPACA_API_KEY || !env.ALPACA_SECRET_KEY) throw new Error("Missing Alpaca credentials. Add them as Cloudflare secrets.");
  const now = new Date();
  const today = nyDate(now);
  const state = await getState(env);
  const account = await alpaca<{ status: string; trading_blocked: boolean; equity: string; cash: string }>(env, "/v2/account");
  if (!account.status.toLowerCase().includes("active") || account.trading_blocked) throw new Error("Live account is not eligible for trading.");
  const equity = Number(account.equity);
  const cash = Number(account.cash);
  if (!(equity > 0) || !(cash >= 0)) throw new Error("Alpaca returned an invalid account balance.");

  if (state.sessionDate !== today || !(state.dayStartEquity > 0)) {
    state.sessionDate = today;
    state.dayStartEquity = equity;
    await putState(env, state);
  }

  const clock = await alpaca<{ is_open: boolean }>(env, "/v2/clock");
  if (!clock.is_open) {
    log(`Regular US market session closed (${today}); no orders.`);
    return;
  }

  const positions = await alpaca<Array<{ symbol: string; qty: string; avg_entry_price: string }>>(env, "/v2/positions");
  if (positions.length > 1) {
    log("More than one account position exists; blocking bot entries.");
    return;
  }
  if (positions.length === 1) {
    const position = positions[0];
    if (!state.active || state.active.symbol !== position.symbol) {
      log(`Untracked live position ${position.symbol}; no new order will be submitted.`);
      return;
    }
    const bracket = await alpaca<AlpacaOrder>(env, `/v2/orders/${encodeURIComponent(state.active.entryOrderId)}?nested=true`);
    const workingStatuses = new Set(["new", "accepted", "pending_new", "partially_filled", "held"]);
    const hasBrokerStop = (bracket.legs ?? []).some((leg) =>
      leg.side === "sell" && ["stop", "stop_limit"].includes((leg.type ?? "").toLowerCase()) && workingStatuses.has(leg.status.toLowerCase())
    );
    if (!hasBrokerStop) {
      console.error(`CRITICAL: ${position.symbol} is open but its Alpaca stop leg is not confirmed active. No new trade will be submitted; inspect Alpaca immediately.`);
      return;
    }
    log(`Open ${position.symbol} position has an active Alpaca stop leg; qty=${position.qty}.`);
    return;
  }

  if (!(await reconcileClosedEntry(env, state, today))) return;
  const orders = await loadOrders(env);
  if (orders.length) {
    log("An open account order exists; blocking new bot entries.");
    return;
  }
  const dailyLoss = (state.dayStartEquity - equity) / state.dayStartEquity;
  if (dailyLoss >= MAX_DAILY_LOSS_PCT) {
    log(`Daily loss cap reached (${(dailyLoss * 100).toFixed(2)}%); entries disabled.`);
    return;
  }
  const tripCount = await recentRoundTrips(env, state, today);
  if (tripCount >= MAX_ROUND_TRIPS_IN_5_SESSIONS) {
    log("Conservative limit reached: 3 completed round trips in the last 5 Alpaca sessions.");
    return;
  }

  const barsBySymbol = await getBars(env, now);
  const spyRows = indicators(barsBySymbol.get("SPY") ?? []);
  const spy = spyRows[spyRows.length - 1];
  if (!spy || spy.ma20 <= spy.ma50) {
    log("SPY trend filter failed; no entry.");
    return;
  }

  let candidate: { symbol: string; score: number; reasons: string[]; price: number } | null = null;
  for (const symbol of WATCHLIST) {
    const result = scoreBars(barsBySymbol.get(symbol) ?? []);
    if (result && result.score >= ENTRY_SCORE_THRESHOLD && result.price > 0 && (!candidate || result.score > candidate.score)) {
      candidate = { symbol, ...result };
    }
  }
  if (!candidate) {
    log("No entry signal.");
    return;
  }

  const capitalCap = numberEnv(env.CAPITAL_CAP_USD, 77.93);
  const maxTradeLoss = numberEnv(env.MAX_TRADE_LOSS_USD, 5.57);
  const maxNotional = Math.min(cash * POSITION_CASH_FRACTION, capitalCap * POSITION_CASH_FRACTION, maxTradeLoss / STOP_LOSS_PCT);
  const qty = Math.floor(maxNotional / candidate.price);
  if (qty < 1) {
    log(`Candidate ${candidate.symbol} costs more than the whole-share risk budget; no fractional bracket order will be used.`);
    return;
  }

  const clientOrderId = `bot-${today.replaceAll("-", "")}-${candidate.symbol.toLowerCase()}`;
  const existing = await alpaca<AlpacaOrder>(env, `/v2/orders:by_client_order_id?client_order_id=${encodeURIComponent(clientOrderId)}`).catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes(": 404 ")) return null;
    throw error;
  });
  if (existing) {
    log(`Idempotency guard found prior order ${existing.id}; refusing to duplicate.`);
    return;
  }

  log(`Candidate ${candidate.symbol} score=${candidate.score} reasons=${candidate.reasons.join(",")}; whole shares=${qty}.`);
  if ((env.ENABLE_LIVE_ORDERS ?? "false").trim().toLowerCase() !== "true") {
    log("Signal-only: ENABLE_LIVE_ORDERS is not true; no order submitted.");
    return;
  }

  const stopPrice = Number((candidate.price * (1 - STOP_LOSS_PCT)).toFixed(2));
  const takeProfitPrice = Number((candidate.price * (1 + TAKE_PROFIT_PCT)).toFixed(2));
  if (!(stopPrice > 0) || !(takeProfitPrice > stopPrice)) throw new Error("Calculated bracket prices are invalid; refusing order.");
  const order = await alpaca<AlpacaOrder>(env, "/v2/orders", {
    method: "POST",
    body: JSON.stringify({
      symbol: candidate.symbol,
      qty: String(qty),
      side: "buy",
      type: "market",
      time_in_force: "gtc",
      order_class: "bracket",
      take_profit: { limit_price: takeProfitPrice.toFixed(2) },
      stop_loss: { stop_price: stopPrice.toFixed(2) },
      client_order_id: clientOrderId,
    }),
  });
  state.active = { symbol: candidate.symbol, entryOrderId: order.id, clientOrderId };
  await putState(env, state);
  log(`LIVE bracket submitted: ${candidate.symbol} x ${qty}; stop=$${stopPrice.toFixed(2)}, target=$${takeProfitPrice.toFixed(2)}, order=${order.id}`);
}

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/healthz") {
      return Response.json({ ok: true, service: "alpaca-cloudflare-worker" });
    }
    return new Response("Not found", { status: 404 });
  },
  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    await run(env).catch((error: unknown) => {
      console.error("Trading cycle failed; no fallback order will be sent.", error);
    });
  },
};

