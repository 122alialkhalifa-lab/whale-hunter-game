import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createHmac, timingSafeEqual, randomUUID } from 'node:crypto';
import WebSocket from 'ws';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 3000;
const AUTH_COOKIE = 'whr_session';
const AUTH_SECRET = process.env.AUTH_SECRET || 'change-this-secret-in-render';
const OWNER_PIN = process.env.OWNER_PIN || 'OWNER-7777';
const VIEWER_PIN = process.env.VIEWER_PIN || 'VIEW-1111';
const REQUIRE_AUTH = String(process.env.REQUIRE_AUTH || 'true').toLowerCase() !== 'false';

const BTC = 'BTCUSDT';
const BINANCE_WS = 'wss://fstream.binance.com/ws/btcusdt@aggTrade';
const BOT_COUNT = Number(process.env.BOT_COUNT || 500);
const INITIAL_FAKE_USD = Number(process.env.INITIAL_FAKE_USD || 1000000);
const PAPER_FEE_BPS = Number(process.env.PAPER_FEE_BPS || 4);
const PAPER_SLIPPAGE_BPS = Number(process.env.PAPER_SLIPPAGE_BPS || 2);
const MIN_NET_PROFIT_PCT = Number(process.env.MIN_NET_PROFIT_PCT || 0.05);
const MAX_NEUTRAL_LOSS_PCT = Number(process.env.MAX_NEUTRAL_LOSS_PCT || 0.05);
const MAX_LEVERAGE = Number(process.env.MAX_FAKE_LEVERAGE || 20);
const MAX_RISK = Number(process.env.MAX_PAPER_RISK || 0.65);
const TRADE_HORIZON_MS = Number(process.env.TRADE_HORIZON_SEC || 600) * 1000;
const EARLY_EXIT_MS = Number(process.env.EARLY_EXIT_SEC || 25) * 1000;
const BOT_TICK_MS = Number(process.env.BOT_TICK_MS || 650);
const THINK_PUSH_MS = Number(process.env.THINK_PUSH_MS || 1400);
const SCAN_WINDOW_MS = Number(process.env.WINDOW_SEC || 60) * 1000;
const WHALE_USD = Number(process.env.WHALE_USD || 30000);
const MIN_CONVICTION = Number(process.env.MIN_CONVICTION || 44);
const TARGET_COMPLETION = Number(process.env.TARGET_COMPLETION || 90);
const SUCCESS_LOG_MAX = Number(process.env.SUCCESS_FORMULA_LOG_MAX || 2000);
const AUTOSTART = String(process.env.AUTOSTART || 'true').toLowerCase() !== 'false';

const SEEDS = Object.freeze({
  UP: {
    id: 'RADAR_001_TAKER_BUY_UP',
    label: 'Radar-001 الغواص / TAKER_BUY / UP',
    direction: 'UP',
    side: 'LONG',
    leverage: 12.3,
    targetNetPct: 0.6171,
    targetGrossPct: 2.0943,
    targetMovePct: 0.1703,
    targetR: 0.7874,
    seedSimilarityFloor: 17,
    originalLine: '[2026-06-30T22:08:20.304Z] BTCUSDT UP | Radar-001 الغواص TAKER_BUY | WIN net=0.6171% gross=2.0943% fees+slip=1.4772% move=0.1703% lev=12.3x | pattern=1/1 PERFECT_SO_FAR | equation: gross=2.0943% - fees=0.9848% - slippage=0.4924% => net=0.6171%; result=WIN; R=0.7874; Δwᵢ=η·R·xᵢ',
    equation: 'gross=2.0943% - fees=0.9848% - slippage=0.4924% => net=0.6171%; result=WIN; R=0.7874; Δwᵢ=η·R·xᵢ'
  },
  DOWN: {
    id: 'RADAR_013_DOGFIGHT_DOWN',
    label: 'Radar-013 المرصاد / DOGFIGHT / DOWN',
    direction: 'DOWN',
    side: 'SHORT',
    leverage: 8,
    targetNetPct: 0.5379,
    targetGrossPct: 1.4970,
    targetMovePct: -0.1871,
    targetR: 0.7250,
    seedSimilarityFloor: 17,
    originalLine: '[2026-06-30T22:38:22.424Z] BTCUSDT DOWN | Radar-013 المرصاد DOGFIGHT | WIN net=0.5379% gross=1.4970% fees+slip=0.9591% move=-0.1871% lev=8x | pattern=1/1 PERFECT_SO_FAR | equation: gross=1.4970% - fees=0.6394% - slippage=0.3197% => net=0.5379%; result=WIN; R=0.7250; Δwᵢ=η·R·xᵢ',
    equation: 'gross=1.4970% - fees=0.6394% - slippage=0.3197% => net=0.5379%; result=WIN; R=0.7250; Δwᵢ=η·R·xᵢ'
  }
});

const COMPONENT_BLUEPRINTS = [
  { key: 'TAKER_BUY', ar: 'ضغط شراء Taker Buy', up: +1, down: -1 },
  { key: 'TAKER_SELL', ar: 'ضغط بيع Taker Sell', up: -1, down: +1 },
  { key: 'BIG_BUY', ar: 'شراء كبير', up: +1, down: -1 },
  { key: 'BIG_SELL', ar: 'بيع كبير', up: -1, down: +1 },
  { key: 'NET_WHALE', ar: 'صافي الحيتان', up: +1, down: -1 },
  { key: 'RANGE_LOCK', ar: 'ضغط النطاق قبل الحركة', up: +1, down: +1 },
  { key: 'PRICE_LIFT', ar: 'ميل السعر الحالي', up: +1, down: -1 },
  { key: 'FLOW', ar: 'انفجار التدفق', up: +1, down: +1 },
  { key: 'WHALE_COUNT', ar: 'عدد صفقات الحيتان', up: +1, down: +1 },
  { key: 'ABSORPTION', ar: 'امتصاص', up: +1, down: -1 },
  { key: 'FAKE_PUMP_RISK', ar: 'خطر فخ سعري', up: -1, down: +1 },
  { key: 'TRADE_SPEED', ar: 'سرعة الصفقات', up: +1, down: +1 }
];

const STYLE_NAMES = ['TAKER_BUY', 'DOGFIGHT', 'RANGE_LOCK', 'NET_WHALE', 'FLOW_BURST', 'ABSORPTION', 'PULSE', 'HYDRA', 'ORBIT', 'TIDE'];

let running = false;
let scanning = false;
let ws = null;
let wsConnected = false;
let wsReconnectTimer = null;
let currentPrice = 0;
let lastTradeAt = 0;
let trades = [];
let sseClients = new Set();
let logLines = [];
let tickTimer = null;
let pushTimer = null;
let activeTrades = [];
let resolvedTrades = [];
let signalDedup = new Set();
let lastSignal = null;
let price24h = { high: 0, low: 0 };

const bots = createBots();
const formulas = createInitialFormulas();
const hive = createHive();

app.disable('x-powered-by');
app.use(express.json({ limit: '128kb' }));
const PUBLIC_DIR = fs.existsSync(path.join(__dirname, 'public', 'index.html')) ? path.join(__dirname, 'public') : __dirname;
app.use(express.static(PUBLIC_DIR, { maxAge: '20m', setHeaders(res, file) { if (file.endsWith('sw.js') || file.endsWith('index.html')) res.setHeader('Cache-Control', 'no-cache'); }}));
app.get('/', (_req, res) => res.sendFile(path.join(PUBLIC_DIR, 'index.html')));

app.get('/health', (_req, res) => res.json({
  ok: true,
  running,
  scanning,
  symbol: BTC,
  bitcoinOnlyMode: true,
  collectiveFormulaCompletion: true,
  equationHunterMode: true,
  twoFormulaOnlyMode: false,
  initialMillionPerBot: INITIAL_FAKE_USD >= 1000000,
  botCount: BOT_COUNT,
  websocketConnected: wsConnected,
  currentPrice,
  lastTradeAt,
  activeTrades: activeTrades.length,
  formulas: formulas.length,
  bestCompletion: bestFormula()?.completion || 0,
  timestamp: Date.now()
}));

app.get('/auth/me', (req, res) => {
  const user = getSessionUser(req);
  res.json({ ok: true, authenticated: Boolean(user), role: user?.role || 'guest', canControl: user?.role === 'owner' || !REQUIRE_AUTH, canView: Boolean(user) || !REQUIRE_AUTH, requireAuth: REQUIRE_AUTH });
});

app.post('/auth/login', (req, res) => {
  const pin = String(req.body?.pin || '').trim();
  let role = null;
  if (!REQUIRE_AUTH) role = 'owner';
  else if (safeEqual(pin, OWNER_PIN)) role = 'owner';
  else if (safeEqual(pin, VIEWER_PIN)) role = 'viewer';
  if (!role) return res.status(401).json({ ok: false, error: 'BAD_PIN' });
  setSessionCookie(res, { role });
  res.json({ ok: true, role });
});

app.post('/auth/logout', (_req, res) => {
  res.setHeader('Set-Cookie', `${AUTH_COOKIE}=; Path=/; Max-Age=0; SameSite=Lax`);
  res.json({ ok: true });
});

app.get('/events', requireViewer, (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();
  const client = { id: randomUUID(), res };
  sseClients.add(client);
  sendEvent(client, 'init', snapshot());
  req.on('close', () => sseClients.delete(client));
});

app.post('/start', requireOwner, (_req, res) => { startLab(); res.json({ ok: true, running }); });
app.post('/stop', requireOwner, (_req, res) => { stopLab(); res.json({ ok: true, running }); });
app.post('/reset', requireOwner, (_req, res) => { resetLab(); res.json({ ok: true }); });
app.post('/scan', requireOwner, (_req, res) => { const sig = scanMarket(); res.json({ ok: true, signal: sig, snapshot: snapshot() }); });

app.get('/formula-success-logs', requireViewer, (req, res) => {
  const limit = clamp(Number(req.query.limit || 300), 1, SUCCESS_LOG_MAX);
  res.json({ ok: true, count: hive.successLogs.length, logs: hive.successLogs.slice(-limit).reverse(), note: 'Paper simulation only. Copy/export before free-host restart if you need permanent history.' });
});

app.get('/formula-success-logs.txt', requireViewer, (req, res) => {
  const limit = clamp(Number(req.query.limit || 500), 1, SUCCESS_LOG_MAX);
  const text = hive.successLogs.slice(-limit).reverse().map(formatSuccessLog).join('\n\n---\n\n') || 'No successful equation logs yet.';
  res.type('text/plain').send(text);
});

app.get('/api/snapshot', requireViewer, (_req, res) => res.json(snapshot()));

function startLab() {
  if (running) return;
  running = true;
  connectBinance();
  if (!tickTimer) tickTimer = setInterval(labTick, BOT_TICK_MS);
  if (!pushTimer) pushTimer = setInterval(() => broadcast('tick', snapshot()), THINK_PUSH_MS);
  log('system', 'V20 Collective Formula Completion started. 500 bots, $1M each, BTCUSDT only.');
}

function stopLab() {
  running = false;
  if (tickTimer) clearInterval(tickTimer);
  if (pushTimer) clearInterval(pushTimer);
  tickTimer = null;
  pushTimer = null;
  log('system', 'Lab stopped by owner. Binance stream may reconnect when started again.');
}

function resetLab() {
  activeTrades = [];
  resolvedTrades = [];
  signalDedup = new Set();
  logLines = [];
  trades = trades.slice(-2000);
  hive.reports = [];
  hive.successLogs = [];
  hive.completionEvents = [];
  hive.rejectedExperiments = [];
  hive.totalExperiments = 0;
  hive.totalWins = 0;
  hive.totalLosses = 0;
  for (const bot of bots) resetBot(bot);
  formulas.splice(0, formulas.length, ...createInitialFormulas());
  log('admin', 'Lab reset. Formula completion restarted from the two seeds.');
  broadcast('reset', snapshot());
}

function connectBinance() {
  if (wsConnected || ws?.readyState === WebSocket.CONNECTING) return;
  try { ws?.terminate?.(); } catch {}
  ws = new WebSocket(BINANCE_WS);
  ws.on('open', () => { wsConnected = true; log('binance', 'Connected to Binance Futures BTCUSDT aggTrade WebSocket.'); broadcast('status', snapshot()); });
  ws.on('message', raw => {
    try {
      const t = JSON.parse(raw.toString());
      const price = Number(t.p);
      const qty = Number(t.q);
      if (!Number.isFinite(price) || !Number.isFinite(qty) || price <= 0) return;
      currentPrice = price;
      lastTradeAt = Number(t.T || Date.now());
      price24h.high = price24h.high ? Math.max(price24h.high, price) : price;
      price24h.low = price24h.low ? Math.min(price24h.low, price) : price;
      trades.push({ ts: lastTradeAt, price, qty, usd: price * qty, buyerAggressive: t.m === false });
      const cutoff = Date.now() - 900000;
      if (trades.length > 20000 || trades[0]?.ts < cutoff) trades = trades.filter(x => x.ts >= cutoff).slice(-20000);
    } catch (err) { log('error', `Bad Binance message: ${err.message}`); }
  });
  ws.on('close', () => { wsConnected = false; log('binance', 'Binance WebSocket disconnected; reconnecting.'); reconnectSoon(); });
  ws.on('error', err => { wsConnected = false; log('binance-error', err.message || String(err)); reconnectSoon(); });
}

function reconnectSoon() {
  if (wsReconnectTimer) return;
  wsReconnectTimer = setTimeout(() => { wsReconnectTimer = null; if (running) connectBinance(); }, 3500);
}

function labTick() {
  if (!running || !currentPrice) return;
  scanning = true;
  const now = Date.now();
  const signal = scanMarket(now);
  lastSignal = signal;
  resolveTrades(now, signal);
  runBotBatch(now, signal);
  evolveFormulaCompletion(now, signal);
  scanning = false;
}

function scanMarket(now = Date.now()) {
  const recent = trades.filter(t => t.ts >= now - SCAN_WINDOW_MS);
  if (!recent.length) return emptySignal(now);
  let buy = 0, sell = 0, bigBuy = 0, bigSell = 0, whaleCount = 0;
  let high = -Infinity, low = Infinity;
  for (const tr of recent) {
    if (tr.buyerAggressive) buy += tr.usd; else sell += tr.usd;
    if (tr.usd >= WHALE_USD) {
      whaleCount += 1;
      if (tr.buyerAggressive) bigBuy += tr.usd; else bigSell += tr.usd;
    }
    high = Math.max(high, tr.price);
    low = Math.min(low, tr.price);
  }
  const first = recent[0].price;
  const last = recent[recent.length - 1].price;
  const total = buy + sell;
  const buyPct = total > 0 ? buy / total : 0.5;
  const priceChangePct = first > 0 ? (last - first) / first * 100 : 0;
  const rangePct = low > 0 ? (high - low) / low * 100 : 0;
  const netWhale = bigBuy - bigSell;
  const absorption = total > WHALE_USD * 3 && buyPct > 0.61 && rangePct < 0.75;
  let watchScore = 0;
  if (total > WHALE_USD * 3) watchScore += 18;
  if (bigBuy > WHALE_USD) watchScore += 18;
  if (bigSell > WHALE_USD) watchScore += 12;
  if (buyPct > 0.64 || buyPct < 0.42) watchScore += 18;
  if (Math.abs(netWhale) > WHALE_USD) watchScore += 15;
  if (absorption) watchScore += 15;
  if (Math.abs(priceChangePct) > 0.12) watchScore += 10;
  return {
    symbol: BTC,
    ts: now,
    price: last,
    buyUsd: buy,
    sellUsd: sell,
    totalFlow: total,
    bigBuyUsd: bigBuy,
    bigSellUsd: bigSell,
    netWhale,
    buyPct,
    priceChangePct,
    priceRangePct: rangePct,
    whaleTradeCount: whaleCount,
    tradeCount: recent.length,
    absorption,
    watchScore: clamp(Math.round(watchScore), 0, 100),
    vector: featureVector({ buy, sell, total, bigBuy, bigSell, netWhale, buyPct, priceChangePct, rangePct, whaleCount, tradeCount: recent.length, absorption })
  };
}

function emptySignal(now) {
  return { symbol: BTC, ts: now, price: currentPrice, buyUsd: 0, sellUsd: 0, totalFlow: 0, bigBuyUsd: 0, bigSellUsd: 0, netWhale: 0, buyPct: 0.5, priceChangePct: 0, priceRangePct: 0, whaleTradeCount: 0, tradeCount: 0, absorption: false, watchScore: 0, vector: {} };
}

function featureVector(x) {
  const flowUnits = x.total / Math.max(1, WHALE_USD * 6);
  const buyImb = (x.buyPct - 0.5) * 2;
  const sellPressure = clamp((x.bigSell / Math.max(1, x.total)) * 5 + Math.max(0, 0.5 - x.buyPct) * 2, 0, 1.8);
  const bigBuy = clamp(x.bigBuy / Math.max(1, WHALE_USD * 3), 0, 2);
  const bigSell = clamp(x.bigSell / Math.max(1, WHALE_USD * 3), 0, 2);
  const netWhale = clamp(x.netWhale / Math.max(1, WHALE_USD * 4), -2, 2);
  const rangeLock = clamp(1 - x.rangePct / 1.2, 0, 1);
  const priceLift = clamp(x.priceChangePct / 0.35, -2, 2);
  return {
    TAKER_BUY: clamp(x.buyPct, 0, 1),
    TAKER_SELL: clamp(1 - x.buyPct, 0, 1),
    BUY_IMBALANCE: buyImb,
    BIG_BUY: bigBuy,
    BIG_SELL: bigSell,
    NET_WHALE: netWhale,
    RANGE_LOCK: rangeLock,
    PRICE_LIFT: priceLift,
    FLOW: clamp(flowUnits, 0, 2),
    WHALE_COUNT: clamp(x.whaleCount / 5, 0, 2),
    ABSORPTION: x.absorption ? 1 : 0,
    SELL_PRESSURE: sellPressure,
    FAKE_PUMP_RISK: clamp(Math.max(0, x.priceChangePct) / 0.25 + bigSell * 0.4, 0, 2),
    TRADE_SPEED: clamp(x.tradeCount / 350, 0, 2)
  };
}

function runBotBatch(now, signal) {
  const idleBots = bots.filter(b => !b.activeTradeId);
  if (!idleBots.length) return;
  const candidates = [];
  const batchSize = Math.min(idleBots.length, Math.max(25, Math.floor(BOT_COUNT / 5)));
  for (let i = 0; i < batchSize; i++) {
    const bot = idleBots[(hive.roundRobin + i) % idleBots.length];
    const idea = botPropose(bot, signal, now);
    if (idea && idea.conviction >= MIN_CONVICTION) candidates.push(idea);
  }
  hive.roundRobin = (hive.roundRobin + batchSize) % Math.max(1, idleBots.length);
  candidates.sort((a, b) => b.priority - a.priority);
  const openLimit = Math.min(80, Math.max(12, Math.floor(BOT_COUNT / 7)));
  for (const idea of candidates.slice(0, openLimit)) openPaperTrade(idea, now, signal);
}

function botPropose(bot, signal, now) {
  const seed = bot.seedBias === 'UP' ? SEEDS.UP : bot.seedBias === 'DOWN' ? SEEDS.DOWN : (Math.random() > 0.5 ? SEEDS.UP : SEEDS.DOWN);
  const parent = chooseFormula(seed.direction, bot);
  const patch = proposePatch(bot, seed.direction, signal.vector || {}, parent);
  const formula = maybeSpawnFormula(parent, patch, bot, now, signal);
  const evaled = evaluateFormula(formula, signal);
  const confidence = Math.round(evaled.conviction + bot.aggression * 4 + bot.reputation * 1.6);
  const isDifferent = dedupKey(formula, signal, Math.floor(now / 10000));
  if (signalDedup.has(isDifferent)) return null;
  signalDedup.add(isDifferent);
  if (signalDedup.size > 1200) signalDedup = new Set([...signalDedup].slice(-500));
  return { bot, formula, seed, direction: formula.direction, conviction: clamp(confidence, 1, 100), similarity: evaled.similarity, patch, priority: evaled.conviction + formula.completion * 0.35 + formula.stats.wins * 0.8, evaled };
}

function chooseFormula(direction, bot) {
  const pool = formulas.filter(f => f.direction === direction && f.status !== 'BROKEN').sort((a, b) => (b.completion + b.stats.wins * 2 - b.stats.losses * 9) - (a.completion + a.stats.wins * 2 - a.stats.losses * 9));
  if (!pool.length) return formulas.find(f => f.direction === direction);
  const top = pool.slice(0, 12);
  if (Math.random() < 0.65 + bot.discipline * 0.02) return top[Math.floor(Math.random() * Math.min(5, top.length))];
  return top[Math.floor(Math.random() * top.length)];
}

function proposePatch(bot, direction, vector, formula) {
  const used = new Set(formula.components.map(c => c.key));
  const available = COMPONENT_BLUEPRINTS.filter(c => !used.has(c.key));
  const blueprint = available.length ? available[Math.floor(Math.random() * available.length)] : COMPONENT_BLUEPRINTS[Math.floor(Math.random() * COMPONENT_BLUEPRINTS.length)];
  const sign = direction === 'UP' ? blueprint.up : blueprint.down;
  const actual = Number(vector[blueprint.key] || 0);
  const target = clamp(actual + sign * (0.10 + Math.random() * 0.28), -2, 2);
  return {
    key: blueprint.key,
    ar: blueprint.ar,
    sign,
    target: Number(target.toFixed(4)),
    tolerance: Number((0.42 + Math.random() * 0.65).toFixed(4)),
    weight: Number((0.45 + bot.curiosity * 0.12 + Math.random() * 1.1).toFixed(4)),
    proposedBy: bot.id
  };
}

function maybeSpawnFormula(parent, patch, bot, now, signal) {
  if (parent.components.length >= 10 && Math.random() > 0.18) return parent;
  const shouldMutate = parent.stats.trials === 0 || Math.random() < 0.38 + bot.curiosity * 0.03 || parent.status === 'BROKEN';
  if (!shouldMutate) return parent;
  const child = {
    id: `${parent.seedId}-${parent.direction}-V${hive.nextFormulaVersion++}`,
    seedId: parent.seedId,
    direction: parent.direction,
    parentId: parent.id,
    createdAt: now,
    createdBy: bot.id,
    generation: parent.generation + 1,
    components: [...parent.components, patch],
    stats: { trials: 0, wins: 0, losses: 0, neutrals: 0, grossProfitUsd: 0, avgNetPct: 0, bestNetPct: 0, worstNetPct: 0 },
    completion: Math.max(parent.completion - 2, seedBaseCompletion(parent.seedId)),
    bestSimilarity: 0,
    status: 'UNDER_TEST',
    equation: buildFormulaEquation(parent.direction, [...parent.components, patch]),
    lastReason: 'New collective patch added by bot scientist.'
  };
  formulas.push(child);
  if (formulas.length > 260) formulas.splice(0, formulas.length - 260);
  pushLimited(hive.completionEvents, { ts: now, type: 'PATCH_ADDED', formulaId: child.id, parentId: parent.id, bot: bot.name, patch, message: `Bot ${bot.id} أضاف قطعة ${patch.key} لإكمال معادلة ${parent.seedId}.` }, 400);
  return child;
}

function evaluateFormula(formula, signal) {
  const vector = signal.vector || {};
  const seed = formula.seedId === SEEDS.UP.id ? SEEDS.UP : SEEDS.DOWN;
  let weighted = 0, total = 0;
  const terms = [];
  for (const c of formula.components) {
    const actual = Number(vector[c.key] || 0);
    const dist = Math.abs(actual - c.target);
    const closeness = clamp(1 - dist / Math.max(0.0001, c.tolerance), 0, 1);
    weighted += closeness * c.weight;
    total += c.weight;
    terms.push({ key: c.key, ar: c.ar, actual: round4(actual), target: c.target, closeness: round2(closeness * 100), weight: c.weight, product: round4(closeness * c.weight) });
  }
  const componentScore = total ? (weighted / total) * 100 : seed.seedSimilarityFloor;
  const sampleBoost = Math.min(18, formula.stats.trials * 0.7);
  const winBoost = formula.stats.trials ? (formula.stats.wins / Math.max(1, formula.stats.trials)) * 22 : 0;
  const lossPenalty = formula.stats.losses * 12;
  const similarity = clamp(seed.seedSimilarityFloor * 0.35 + componentScore * 0.45 + sampleBoost + winBoost - lossPenalty, 0, 100);
  const conviction = clamp(similarity + signal.watchScore * 0.22 + Math.min(12, Math.abs(signal.priceChangePct) * 20), 0, 100);
  return { similarity: round2(similarity), conviction: round2(conviction), componentScore: round2(componentScore), terms };
}

function openPaperTrade(idea, now, signal) {
  const bot = idea.bot;
  if (!currentPrice || bot.balance <= 0 || bot.activeTradeId) return null;
  const lev = chooseLeverage(idea);
  const riskPct = clamp((0.04 + idea.conviction / 150 + bot.aggression / 180), 0.06, MAX_RISK);
  const margin = bot.balance * riskPct;
  const notional = margin * lev;
  const trade = {
    id: randomUUID(),
    symbol: BTC,
    openedAt: now,
    resolveAt: now + TRADE_HORIZON_MS,
    earliestExitAt: now + EARLY_EXIT_MS,
    entryPrice: currentPrice,
    exitPrice: null,
    direction: idea.direction,
    side: idea.direction === 'UP' ? 'LONG' : 'SHORT',
    leverage: lev,
    marginUsd: margin,
    notionalUsd: notional,
    botId: bot.id,
    botName: bot.name,
    formulaId: idea.formula.id,
    seedId: idea.formula.seedId,
    completionAtEntry: round2(idea.formula.completion),
    similarityAtEntry: round2(idea.similarity),
    conviction: idea.conviction,
    equation: idea.formula.equation,
    components: idea.formula.components.map(x => ({ ...x })),
    entryVector: { ...(signal.vector || {}) },
    status: 'OPEN'
  };
  activeTrades.push(trade);
  bot.activeTradeId = trade.id;
  bot.pending += 1;
  hive.totalExperiments += 1;
  pushLimited(hive.reports, {
    ts: now,
    type: 'EXPERIMENT_OPEN',
    bot: bot.name,
    formulaId: trade.formulaId,
    seedId: trade.seedId,
    direction: trade.direction,
    similarity: trade.similarityAtEntry,
    completion: trade.completionAtEntry,
    equation: trade.equation,
    message: `فتح تجربة وهمية: ${trade.direction} بتشابه ${trade.similarityAtEntry}% لإكمال ${trade.seedId}.`
  }, 250);
  return trade;
}

function resolveTrades(now, signal) {
  if (!activeTrades.length || !currentPrice) return;
  const still = [];
  for (const tr of activeTrades) {
    const preview = computePaperResult(tr, currentPrice, now, false);
    const earlyWin = now >= tr.earliestExitAt && preview.netPnlPct > MIN_NET_PROFIT_PCT * 1.8;
    const earlyLoss = now >= tr.earliestExitAt && preview.netPnlPct < -MAX_NEUTRAL_LOSS_PCT * 2.2;
    if (now >= tr.resolveAt || earlyWin || earlyLoss) closeTrade(tr, currentPrice, now, signal); else still.push(tr);
  }
  activeTrades = still;
}

function closeTrade(tr, exitPrice, now, signal) {
  const bot = bots[tr.botId - 1];
  tr.exitPrice = exitPrice;
  tr.closedAt = now;
  tr.status = 'CLOSED';
  const result = computePaperResult(tr, exitPrice, now, true);
  tr.result = result;
  bot.activeTradeId = null;
  bot.pending = Math.max(0, bot.pending - 1);
  bot.closed += 1;
  if (result.result === 'WIN') { bot.wins += 1; hive.totalWins += 1; }
  if (result.result === 'LOSS') { bot.losses += 1; hive.totalLosses += 1; }
  bot.balance += result.profitUsd;
  bot.reputation = clamp(bot.reputation + (result.result === 'WIN' ? 0.8 : result.result === 'LOSS' ? -1.2 : -0.1), -30, 60);
  const formula = formulas.find(f => f.id === tr.formulaId);
  if (formula) updateFormulaAfterTrade(formula, tr, result, now, signal);
  resolvedTrades.push(tr);
  if (resolvedTrades.length > 1500) resolvedTrades = resolvedTrades.slice(-1500);
  if (result.result === 'WIN') recordSuccessLog(tr, result, formula, now);
  else if (result.result === 'LOSS') pushLimited(hive.rejectedExperiments, { ts: now, formulaId: tr.formulaId, result, reason: diagnoseLoss(tr, result, signal) }, 300);
}

function computePaperResult(tr, exitPrice, now, final) {
  const movePct = tr.entryPrice > 0 ? ((exitPrice - tr.entryPrice) / tr.entryPrice) * 100 : 0;
  const grossPnlPct = tr.side === 'SHORT' ? -movePct * tr.leverage : movePct * tr.leverage;
  const feesPct = (2 * PAPER_FEE_BPS / 100) * tr.leverage;
  const slippagePct = (2 * PAPER_SLIPPAGE_BPS / 100) * tr.leverage;
  const netPnlPct = grossPnlPct - feesPct - slippagePct;
  let result = 'NEUTRAL';
  if (netPnlPct > MIN_NET_PROFIT_PCT) result = 'WIN';
  else if (netPnlPct < -MAX_NEUTRAL_LOSS_PCT) result = 'LOSS';
  const R = result === 'WIN' ? Math.abs(movePct) + Math.abs(netPnlPct) : result === 'LOSS' ? -(Math.abs(movePct) + Math.abs(netPnlPct)) : 0;
  const profitUsd = tr.marginUsd * netPnlPct / 100;
  return {
    final,
    result,
    movePct: round4(movePct),
    grossPnlPct: round4(grossPnlPct),
    feesPct: round4(feesPct),
    slippagePct: round4(slippagePct),
    netPnlPct: round4(netPnlPct),
    R: round4(R),
    profitUsd: round2(profitUsd),
    durationSec: Math.round((now - tr.openedAt) / 1000),
    proofEligible: true,
    equation: `gross=${round4(grossPnlPct)}% - fees=${round4(feesPct)}% - slippage=${round4(slippagePct)}% => net=${round4(netPnlPct)}%; result=${result}; R=${round4(R)}; Δwᵢ=η·R·xᵢ`
  };
}

function updateFormulaAfterTrade(formula, tr, result, now, signal) {
  const s = formula.stats;
  s.trials += 1;
  if (result.result === 'WIN') s.wins += 1;
  else if (result.result === 'LOSS') s.losses += 1;
  else s.neutrals += 1;
  s.grossProfitUsd = round2(s.grossProfitUsd + result.profitUsd);
  s.avgNetPct = round4(((s.avgNetPct * (s.trials - 1)) + result.netPnlPct) / s.trials);
  s.bestNetPct = Math.max(s.bestNetPct || -999, result.netPnlPct);
  s.worstNetPct = Math.min(s.worstNetPct || 999, result.netPnlPct);
  const evaled = evaluateFormula(formula, signal || lastSignal || emptySignal(now));
  formula.bestSimilarity = Math.max(formula.bestSimilarity || 0, evaled.similarity);
  const zeroLossBonus = s.losses === 0 && s.wins > 0 ? Math.min(22, s.wins * 2.4) : 0;
  const sampleBoost = Math.min(18, s.trials * 0.55);
  const winRate = s.trials ? s.wins / s.trials : 0;
  formula.completion = round2(clamp(Math.max(formula.completion, evaled.similarity) + zeroLossBonus + sampleBoost + winRate * 8 - s.losses * 18, 0, 100));
  formula.status = statusFromStats(s, formula.completion);
  if (result.result === 'WIN') {
    reinforceComponents(formula, tr.entryVector, result.R);
    pushLimited(hive.completionEvents, { ts: now, type: 'WIN_REINFORCE', formulaId: formula.id, completion: formula.completion, stats: { ...s }, result, message: `معادلة ${formula.id} ربحت صافيًا، تم تثبيت القطع الناجحة.` }, 400);
  } else if (result.result === 'LOSS') {
    weakenComponents(formula, tr.entryVector, result.R);
    pushLimited(hive.completionEvents, { ts: now, type: 'LOSS_REVIEW', formulaId: formula.id, completion: formula.completion, stats: { ...s }, result, message: `معادلة ${formula.id} فشلت، الخلية ستولد نسخة معدلة.` }, 400);
    if (s.losses > 0) formula.status = 'BROKEN';
  }
  formula.equation = buildFormulaEquation(formula.direction, formula.components);
}

function reinforceComponents(formula, vector, R) {
  for (const c of formula.components) {
    const actual = Number(vector[c.key] || 0);
    c.target = round4(c.target * 0.78 + actual * 0.22);
    c.weight = round4(clamp(c.weight + Math.abs(R) * 0.04, 0.2, 4));
    c.tolerance = round4(clamp(c.tolerance * 0.98, 0.18, 1.4));
  }
}

function weakenComponents(formula, _vector, R) {
  for (const c of formula.components) {
    c.weight = round4(clamp(c.weight - Math.abs(R) * 0.05, 0.15, 4));
    c.tolerance = round4(clamp(c.tolerance * 1.08, 0.2, 1.8));
  }
}

function evolveFormulaCompletion(now, signal) {
  const bestUp = bestFormula('UP');
  const bestDown = bestFormula('DOWN');
  const top = [bestUp, bestDown].filter(Boolean);
  for (const f of top) {
    if (f && f.completion >= TARGET_COMPLETION && f.stats.losses === 0 && f.stats.wins >= 10) {
      pushLimited(hive.reports, { ts: now, type: 'PERFECT_SO_FAR_ALERT', formulaId: f.id, direction: f.direction, completion: f.completion, stats: { ...f.stats }, equation: f.equation, message: `👑 نمط قوي لم يفشل حتى الآن: ${f.id} completion=${f.completion}%` }, 250);
    }
  }
}

function createInitialFormulas() {
  const base = [];
  for (const seed of [SEEDS.UP, SEEDS.DOWN]) {
    const dir = seed.direction;
    const starter = [
      makeComponent(dir, dir === 'UP' ? 'TAKER_BUY' : 'TAKER_SELL', 0.60, 0.65, 1.4),
      makeComponent(dir, dir === 'UP' ? 'PRICE_LIFT' : 'PRICE_LIFT', dir === 'UP' ? 0.18 : -0.18, 0.85, 1.2),
      makeComponent(dir, dir === 'UP' ? 'BIG_BUY' : 'BIG_SELL', 0.48, 0.9, 1.1)
    ];
    base.push({
      id: `${seed.id}-V0`,
      seedId: seed.id,
      direction: dir,
      parentId: null,
      createdAt: Date.now(),
      createdBy: 0,
      generation: 0,
      components: starter,
      stats: { trials: 0, wins: 0, losses: 0, neutrals: 0, grossProfitUsd: 0, avgNetPct: 0, bestNetPct: 0, worstNetPct: 0 },
      completion: seedBaseCompletion(seed.id),
      bestSimilarity: seedBaseCompletion(seed.id),
      status: 'SEED_INCOMPLETE',
      equation: buildFormulaEquation(dir, starter),
      lastReason: 'Seed from original winning result, still missing entry-condition components.'
    });
  }
  return base;
}

function makeComponent(direction, key, target, tolerance, weight) {
  const bp = COMPONENT_BLUEPRINTS.find(x => x.key === key) || { key, ar: key, up: 1, down: 1 };
  return { key, ar: bp.ar, sign: direction === 'UP' ? bp.up : bp.down, target, tolerance, weight, proposedBy: 0 };
}

function seedBaseCompletion(seedId) {
  return seedId === SEEDS.UP.id ? SEEDS.UP.seedSimilarityFloor : SEEDS.DOWN.seedSimilarityFloor;
}

function statusFromStats(s, completion) {
  if (s.losses > 0) return 'BROKEN';
  if (s.wins >= 50 && completion >= 90) return 'PERFECT_SO_FAR';
  if (s.wins >= 25) return 'KILLER_CANDIDATE';
  if (s.wins >= 10) return 'PROMISING';
  if (s.wins >= 3) return 'UNDER_TEST';
  return 'SEED_INCOMPLETE';
}

function buildFormulaEquation(direction, components) {
  const sign = direction === 'UP' ? 'UP' : 'DOWN';
  const parts = components.map(c => `${c.weight >= 0 ? '+' : '-'} ${Math.abs(c.weight).toFixed(2)}·${c.key}[target=${c.target}]`);
  return `${sign}_COMPLETION = Seed + ${parts.join(' ')} ; trade only after paper-proof, fees/slip included`;
}

function chooseLeverage(idea) {
  const seedLev = idea.seed.leverage || 8;
  const completionLev = idea.formula.completion > 70 ? 3 : 0;
  const convictionLev = (idea.conviction - 50) / 10;
  return round2(clamp(seedLev + completionLev + convictionLev, 1, MAX_LEVERAGE));
}

function dedupKey(formula, signal, bucket) {
  const v = signal.vector || {};
  const keys = ['TAKER_BUY', 'BIG_BUY', 'BIG_SELL', 'NET_WHALE', 'RANGE_LOCK', 'PRICE_LIFT', 'FLOW'];
  const rounded = keys.map(k => `${k}:${Math.round((v[k] || 0) * 10)}`).join('|');
  return `${BTC}|${formula.direction}|${formula.id}|${bucket}|${rounded}`;
}

function createBots() {
  return Array.from({ length: BOT_COUNT }, (_, i) => ({
    id: i + 1,
    name: `Scientist-${String(i + 1).padStart(3, '0')}`,
    style: STYLE_NAMES[i % STYLE_NAMES.length],
    seedBias: i % 3 === 0 ? 'UP' : i % 3 === 1 ? 'DOWN' : 'MIXED',
    balance: INITIAL_FAKE_USD,
    activeTradeId: null,
    pending: 0,
    closed: 0,
    wins: 0,
    losses: 0,
    reputation: 0,
    aggression: 3 + (i % 10),
    curiosity: 3 + ((i * 7) % 10),
    discipline: 2 + ((i * 11) % 10)
  }));
}

function resetBot(bot) {
  bot.balance = INITIAL_FAKE_USD;
  bot.activeTradeId = null;
  bot.pending = 0;
  bot.closed = 0;
  bot.wins = 0;
  bot.losses = 0;
  bot.reputation = 0;
}

function createHive() {
  return { reports: [], successLogs: [], completionEvents: [], rejectedExperiments: [], totalExperiments: 0, totalWins: 0, totalLosses: 0, roundRobin: 0, nextFormulaVersion: 1 };
}

function bestFormula(direction = null) {
  const pool = direction ? formulas.filter(f => f.direction === direction) : formulas;
  return [...pool].sort((a, b) => (b.completion + b.stats.wins * 1.3 - b.stats.losses * 20) - (a.completion + a.stats.wins * 1.3 - a.stats.losses * 20))[0] || null;
}

function recordSuccessLog(tr, result, formula, now) {
  const logEntry = {
    id: randomUUID(), ts: now, time: new Date(now).toISOString(), symbol: BTC, bot: tr.botName,
    direction: tr.direction, leverage: tr.leverage, marginUsd: round2(tr.marginUsd), notionalUsd: round2(tr.notionalUsd),
    formulaId: tr.formulaId, seedId: tr.seedId, completion: formula?.completion || tr.completionAtEntry, similarity: tr.similarityAtEntry,
    entryPrice: tr.entryPrice, exitPrice: tr.exitPrice,
    movePct: result.movePct, grossPnlPct: result.grossPnlPct, feesPct: result.feesPct, slippagePct: result.slippagePct, netPnlPct: result.netPnlPct,
    profitUsd: result.profitUsd, result: result.result, R: result.R,
    equation: result.equation,
    discoveryEquation: formula?.equation || tr.equation,
    components: tr.components,
    seedOriginalLine: tr.seedId === SEEDS.UP.id ? SEEDS.UP.originalLine : SEEDS.DOWN.originalLine,
    note: 'WIN is counted only because net PnL after fees/slippage is greater than MIN_NET_PROFIT_PCT. Paper simulation only.'
  };
  pushLimited(hive.successLogs, logEntry, SUCCESS_LOG_MAX);
}

function formatSuccessLog(l) {
  return `[${l.time}] ${l.symbol} ${l.direction} | ${l.bot} | WIN net=${l.netPnlPct}% gross=${l.grossPnlPct}% fees=${l.feesPct}% slippage=${l.slippagePct}% move=${l.movePct}% lev=${l.leverage}x | formula=${l.formulaId} completion=${l.completion}% similarity=${l.similarity}% | equation: ${l.equation}\nDISCOVERY: ${l.discoveryEquation}\nSEED: ${l.seedOriginalLine}`;
}

function diagnoseLoss(tr, result, signal) {
  const reasons = [];
  if (result.netPnlPct <= 0 && Math.abs(result.grossPnlPct) < result.feesPct + result.slippagePct) reasons.push('الحركة لم تغطِ الرسوم والانزلاق.');
  if (tr.direction === 'UP' && (signal?.vector?.SELL_PRESSURE || 0) > 0.8) reasons.push('Sell pressure عكس معادلة UP.');
  if (tr.direction === 'DOWN' && (signal?.vector?.TAKER_BUY || 0) > 0.62) reasons.push('Taker buy عكس معادلة DOWN.');
  if (tr.leverage > 14) reasons.push('رافعة وهمية عالية رفعت حساسية الخسارة.');
  return reasons.join(' ') || 'فشل رياضي؛ سيتم تخفيض أوزان القطع المرتبطة.';
}

function snapshot() {
  const sig = lastSignal || emptySignal(Date.now());
  const topFormulas = [...formulas].sort((a, b) => (b.completion + b.stats.wins * 1.5 - b.stats.losses * 18) - (a.completion + a.stats.wins * 1.5 - a.stats.losses * 18)).slice(0, 20);
  const leaderboard = [...bots].sort((a, b) => b.balance - a.balance).slice(0, 20).map(b => ({ id: b.id, name: b.name, balance: round2(b.balance), pnlUsd: round2(b.balance - INITIAL_FAKE_USD), wins: b.wins, losses: b.losses, active: Boolean(b.activeTradeId), reputation: round2(b.reputation), style: b.style }));
  return {
    ok: true,
    service: 'Whale Hunter Bitcoin Collective Formula Completion V20',
    mode: 'V20_COLLECTIVE_FORMULA_COMPLETION',
    running,
    scanning,
    symbol: BTC,
    price: currentPrice,
    wsConnected,
    lastTradeAt,
    botCount: BOT_COUNT,
    initialFakeUsd: INITIAL_FAKE_USD,
    totalFakeTreasury: BOT_COUNT * INITIAL_FAKE_USD,
    targetCompletion: TARGET_COMPLETION,
    seeds: Object.values(SEEDS),
    signal: sig,
    summary: {
      activeTrades: activeTrades.length,
      resolvedTrades: resolvedTrades.length,
      formulas: formulas.length,
      totalExperiments: hive.totalExperiments,
      wins: hive.totalWins,
      losses: hive.totalLosses,
      bestCompletion: bestFormula()?.completion || 0,
      successLogs: hive.successLogs.length
    },
    bestUp: bestFormula('UP'),
    bestDown: bestFormula('DOWN'),
    formulas: topFormulas,
    activeTrades: activeTrades.slice(-80).map(t => ({ ...t, preview: computePaperResult(t, currentPrice || t.entryPrice, Date.now(), false) })),
    recentClosed: resolvedTrades.slice(-80).reverse(),
    leaderboard,
    reports: hive.reports.slice(-80).reverse(),
    completionEvents: hive.completionEvents.slice(-80).reverse(),
    successLogs: hive.successLogs.slice(-80).reverse(),
    rejectedExperiments: hive.rejectedExperiments.slice(-40).reverse(),
    logs: logLines.slice(-90),
    timestamp: Date.now()
  };
}

function log(type, message) {
  const item = { ts: Date.now(), time: new Date().toISOString(), type, message };
  logLines.push(item);
  if (logLines.length > 500) logLines = logLines.slice(-500);
  broadcast('log', item);
}

function broadcast(event, data) {
  for (const client of sseClients) sendEvent(client, event, data);
}

function sendEvent(client, event, data) {
  try { client.res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); } catch { sseClients.delete(client); }
}

function setSessionCookie(res, payload) {
  const body = Buffer.from(JSON.stringify({ ...payload, exp: Date.now() + 1000 * 60 * 60 * 24 * 14 })).toString('base64url');
  const sig = createHmac('sha256', AUTH_SECRET).update(body).digest('base64url');
  res.setHeader('Set-Cookie', `${AUTH_COOKIE}=${body}.${sig}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${60 * 60 * 24 * 14}`);
}

function getSessionUser(req) {
  if (!REQUIRE_AUTH) return { role: 'owner' };
  const cookie = String(req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith(`${AUTH_COOKIE}=`));
  if (!cookie) return null;
  const value = cookie.slice(AUTH_COOKIE.length + 1);
  const [body, sig] = value.split('.');
  if (!body || !sig) return null;
  const expected = createHmac('sha256', AUTH_SECRET).update(body).digest('base64url');
  if (!safeEqual(sig, expected)) return null;
  try {
    const parsed = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    if (!parsed.exp || parsed.exp < Date.now()) return null;
    return { role: parsed.role === 'owner' ? 'owner' : 'viewer' };
  } catch { return null; }
}

function requireViewer(req, res, next) {
  if (!REQUIRE_AUTH) return next();
  const user = getSessionUser(req);
  if (!user) return res.status(401).json({ ok: false, error: 'AUTH_REQUIRED' });
  next();
}

function requireOwner(req, res, next) {
  if (!REQUIRE_AUTH) return next();
  const user = getSessionUser(req);
  if (!user) return res.status(401).json({ ok: false, error: 'AUTH_REQUIRED' });
  if (user.role !== 'owner') return res.status(403).json({ ok: false, error: 'OWNER_ONLY' });
  next();
}

function safeEqual(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  return ab.length === bb.length && timingSafeEqual(ab, bb);
}

function pushLimited(arr, item, max) { arr.push(item); if (arr.length > max) arr.splice(0, arr.length - max); }
function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, Number.isFinite(n) ? n : lo)); }
function round2(n) { return Number(Number(n || 0).toFixed(2)); }
function round4(n) { return Number(Number(n || 0).toFixed(4)); }

app.listen(PORT, () => {
  console.log(`Whale Hunter Bitcoin Collective Formula Completion V20 running on :${PORT}`);
  log('system', `Server listening on port ${PORT}.`);
  if (AUTOSTART) startLab();
});
