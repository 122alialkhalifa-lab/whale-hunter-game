import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import { randomUUID, createHmac, timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
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
const MIN_PROOF_SAMPLES = Number(process.env.MIN_PROOF_SAMPLES || 25);
const PERFECT_ALERT_ONLY = String(process.env.PERFECT_ALERT_ONLY || 'true').toLowerCase() !== 'false';
const BINANCE_BASE = 'https://fapi.binance.com';
const BINANCE_WS_STREAM_BASE = 'wss://fstream.binance.com/market/stream?streams=';
const BINANCE_WS_DISCOVERY_URL = 'wss://fstream.binance.com/market/ws/!miniTicker@arr';
const ALL_SYMBOLS_SENTINEL = 'ALL_BINANCE_USDT';
const BTC_ONLY_SYMBOL = 'BTCUSDT';
const BITCOIN_ONLY_MODE = true;
const TWO_FORMULA_ONLY_MODE = true;
const FIXED_FORMULA_LIBRARY = Object.freeze({
  UP_TAKER_BUY: {
    id: 'UP_TAKER_BUY_RADAR_001',
    styleKey: 'TAKER_BUY',
    originalBot: 'Radar-001 الغواص',
    direction: 'UP',
    side: 'LONG',
    leverage: 12.3,
    originalLine: '[2026-06-30T22:08:20.304Z] BTCUSDT UP | Radar-001 الغواص TAKER_BUY | WIN net=0.6171% gross=2.0943% fees+slip=1.4772% move=0.1703% lev=12.3x | pattern=1/1 PERFECT_SO_FAR | equation: gross=2.0943% - fees=0.9848% - slippage=0.4924% => net=0.6171%; result=WIN; R=0.7874; Δwᵢ=η·R·xᵢ',
    equation: 'gross=2.0943% - fees=0.9848% - slippage=0.4924% => net=0.6171%; result=WIN; R=0.7874; Δwᵢ=η·R·xᵢ',
    entryNote: 'Fixed formula only: BTCUSDT UP / TAKER_BUY / leverage 12.3x.'
  },
  DOWN_DOGFIGHT: {
    id: 'DOWN_DOGFIGHT_RADAR_013',
    styleKey: 'DOGFIGHT',
    originalBot: 'Radar-013 المرصاد',
    direction: 'DOWN',
    side: 'SHORT',
    leverage: 8,
    originalLine: '[2026-06-30T22:38:22.424Z] BTCUSDT DOWN | Radar-013 المرصاد DOGFIGHT | WIN net=0.5379% gross=1.4970% fees+slip=0.9591% move=-0.1871% lev=8x | pattern=1/1 PERFECT_SO_FAR | equation: gross=1.4970% - fees=0.6394% - slippage=0.3197% => net=0.5379%; result=WIN; R=0.7250; Δwᵢ=η·R·xᵢ',
    equation: 'gross=1.4970% - fees=0.6394% - slippage=0.3197% => net=0.5379%; result=WIN; R=0.7250; Δwᵢ=η·R·xᵢ',
    entryNote: 'Fixed formula only: BTCUSDT DOWN / DOGFIGHT / leverage 8x.'
  }
});
const FIXED_FORMULAS = Object.freeze([FIXED_FORMULA_LIBRARY.UP_TAKER_BUY, FIXED_FORMULA_LIBRARY.DOWN_DOGFIGHT]);
const MAX_SYMBOLS = 1;
const WS_SUBSCRIBE_CHUNK = Number(process.env.WS_SUBSCRIBE_CHUNK || 180);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 9000);
const SYMBOL_DELAY_MS = Number(process.env.SYMBOL_DELAY_MS || 20);
const TRADE_BUFFER_MAX_MS = Number(process.env.TRADE_BUFFER_MAX_MS || 900000);
const WS_RECONNECT_MS = Number(process.env.WS_RECONNECT_MS || 5000);
const CONTESTANT_COUNT = 500;
const INITIAL_FAKE_USD = Number(process.env.INITIAL_FAKE_USD || 1000);
const PAPER_FEE_BPS = Number(process.env.PAPER_FEE_BPS || 4);
const PAPER_SLIPPAGE_BPS = Number(process.env.PAPER_SLIPPAGE_BPS || 2);
const MIN_NET_PROFIT_PCT = Number(process.env.MIN_NET_PROFIT_PCT || 0.05);
const MAX_NEUTRAL_LOSS_PCT = Number(process.env.MAX_NEUTRAL_LOSS_PCT || 0.05);
const MAX_PAPER_RISK = Number(process.env.MAX_PAPER_RISK || 0.34);
const LESSON_LOOKBACK_MS = Number(process.env.LESSON_LOOKBACK_MS || 180000);
const BOT_TICK_MS = Number(process.env.BOT_TICK_MS || 1100);
const AUTOSTART = String(process.env.AUTOSTART || 'true').toLowerCase() !== 'false';
const LIVE_HIVE_PUSH_MS = Number(process.env.LIVE_HIVE_PUSH_MS || 3000);
const HIVE_LEARNING_RATE = Number(process.env.HIVE_LEARNING_RATE || 0.035);
const HIVE_REPORT_MAX = Number(process.env.HIVE_REPORT_MAX || 220);
const SUCCESS_FORMULA_LOG_MAX = Number(process.env.SUCCESS_FORMULA_LOG_MAX || 1500);
const SCIENTIST_THINK_MS = Number(process.env.SCIENTIST_THINK_MS || 1500);
const LIVE_THINK_SAMPLE = Number(process.env.LIVE_THINK_SAMPLE || 18);
const MICRO_LEARN_RATE = Number(process.env.MICRO_LEARN_RATE || 0.018);
const MAX_FAKE_LEVERAGE = Number(process.env.MAX_FAKE_LEVERAGE || 20);
const MIN_FAKE_LEVERAGE = Number(process.env.MIN_FAKE_LEVERAGE || 1);
const LIQUIDATION_MARGIN_RATIO = Number(process.env.LIQUIDATION_MARGIN_RATIO || 0.88);
let liveHiveTimer = null;
let scientistThinkTimer = null;

const defaultConfig = Object.freeze({
  // Bitcoin-only mode: every bot, pattern, equation, log, and alert is locked to BTCUSDT.
  symbols: [BTC_ONLY_SYMBOL],
  whaleUsd: 30000,
  minScore: 70,
  windowSec: 60,
  intervalSec: 5,
  arenaHorizonSec: 600,
  maxFakeLeverage: 12.3
});

const strategyDeck = Object.freeze([
  { key: 'TAKER_BUY', name: 'Taker Buy Falcon', ar: 'صقر الشراء', indicator: 'Buy %', direction: 'UP', desc: 'يركز على سيطرة المشترين العدوانيين.' },
  { key: 'BIG_BUY', name: 'Big Buy Sniper', ar: 'قناص الصفقة الكبيرة', indicator: 'Big Buy USD', direction: 'UP', desc: 'يصطاد الصفقات الفردية الكبيرة.' },
  { key: 'NET_WHALE', name: 'Net Whale Diver', ar: 'غواص صافي الحوت', indicator: 'Net Whale', direction: 'UP', desc: 'يقارن الحوت المشتري بالحوت البائع.' },
  { key: 'ABSORPTION', name: 'Absorption Hunter', ar: 'صياد الامتصاص', indicator: 'Absorption', direction: 'UP', desc: 'يبحث عن شراء قوي مع نطاق سعر مضغوط.' },
  { key: 'PRICE_LIFT', name: 'Momentum Rider', ar: 'راكب الزخم', indicator: 'Price Change', direction: 'UP', desc: 'يراهن على الرفع السعري بعد تدفق الشراء.' },
  { key: 'FLOW_BURST', name: 'Flow Burst Scout', ar: 'كشاف التدفق', indicator: 'Total Flow', direction: 'UP', desc: 'يراقب انفجار السيولة في النافذة.' },
  { key: 'WHALE_COUNT', name: 'Whale Count Keeper', ar: 'عداد الحيتان', indicator: 'Whale Trades', direction: 'UP', desc: 'يقيس عدد الصفقات التي تتجاوز العتبة.' },
  { key: 'RANGE_LOCK', name: 'Range Lock Radar', ar: 'رادار النطاق', indicator: 'Compressed Range', direction: 'UP', desc: 'يفضل ضغط السعر مع تدفق قوي.' },
  { key: 'SELL_PRESSURE', name: 'Red Tide Sentinel', ar: 'حارس المد الأحمر', indicator: 'Big Sell USD', direction: 'DOWN', desc: 'يتتبع ضغط البيع الكبير كتحذير.' },
  { key: 'TRAP_REVERSAL', name: 'Trap Reversal Fox', ar: 'ثعلب الفخ', indicator: 'Sell Trap + Buy %', direction: 'UP', desc: 'يبحث عن بيع كبير يتم امتصاصه بالشراء.' },
  { key: 'SCORE_PURITY', name: 'Signal Purist', ar: 'نقي الإشارة', indicator: 'Score', direction: 'UP', desc: 'يتبع أعلى Score فقط.' },
  { key: 'LOW_RANGE_BOSS', name: 'Silent Boss Seeker', ar: 'باحث الزعيم الصامت', indicator: 'High Score + Low Range', direction: 'UP', desc: 'يبحث عن Boss هادئ قبل الانفجار.' },
  { key: 'DOGFIGHT', name: 'Dogfight Reader', ar: 'قارئ المعركة', indicator: 'Buy/Sell Battle', direction: 'WATCH', desc: 'يتوقع مراقبة إذا كانت المعركة متقاربة.' },
  { key: 'MICRO_LIFT', name: 'Micro Lift Sensor', ar: 'حساس الرفعة الصغيرة', indicator: 'Small Price Lift', direction: 'UP', desc: 'يلتقط التحول البسيط في السعر.' },
  { key: 'HEAVY_SYMBOL', name: 'Heavy Sector Captain', ar: 'قائد القطاع الثقيل', indicator: 'BTC/ETH/SOL Bias', direction: 'UP', desc: 'يفضل الرموز الثقيلة عند وجود تدفق.' },
  { key: 'ALT_HUNTER', name: 'Alt Whale Chaser', ar: 'مطارد حيتان البدائل', indicator: 'Alt Flow', direction: 'UP', desc: 'يعطي أفضلية للرموز البديلة ذات الحركة العالية.' },
  { key: 'SELL_FADE', name: 'Sell Fade Monk', ar: 'راهب تلاشي البيع', indicator: 'Sell Pressure Fade', direction: 'WATCH', desc: 'لا يطارد الشراء إذا البيع أعلى من اللازم.' },
  { key: 'IMBALANCE_PLUS', name: 'Imbalance Alchemist', ar: 'خيميائي الاختلال', indicator: 'Buy % + Net Whale', direction: 'UP', desc: 'يمزج الاختلال وصافي الحيتان.' },
  { key: 'BOSS_ONLY', name: 'Boss Gatekeeper', ar: 'حارس الزعيم', indicator: 'Score 90+', direction: 'UP', desc: 'لا يتحمس إلا للإشارات النادرة جدًا.' },
  { key: 'CALM_WATCHER', name: 'Calm Watch Oracle', ar: 'عرّاف الهدوء', indicator: 'Neutral / Watch', direction: 'WATCH', desc: 'يتوقع مراقبة إذا لا توجد قراءة نظيفة.' }
]);

let config = structuredClone(defaultConfig);
let running = false;
let scanTimer = null;
let scanning = false;
let lastScan = null;
let validSymbols = new Set();
let exchangeInfoLoadedAt = null;
let sseClients = new Set();
let logBuffer = [];
let currentPrices = new Map();
let tradeBuffers = new Map();
let wsState = {
  ws: null,
  key: '',
  mode: 'idle',
  connected: false,
  lastMessageAt: null,
  reconnectTimer: null,
  url: null,
  subscribedSymbols: new Set(),
  discoveredSymbols: new Set(),
  pendingSubscribe: []
};
let resolvedPredictions = [];
let activePredictions = [];
let botTimer = null;
let lastBotTickAt = Date.now();
const contestants = createContestants(CONTESTANT_COUNT);
const hiveMemory = createHiveMemory();
let symbolSnapshots = new Map();
let lastLessonAt = new Map();

app.disable('x-powered-by');
app.use(express.json({ limit: '96kb' }));

// iPad/GitHub web uploads sometimes flatten the /public folder.
// This server supports both layouts:
// 1) normal: /public/index.html
// 2) iPad easy upload: /index.html at repository root
const PUBLIC_DIR = fs.existsSync(path.join(__dirname, 'public', 'index.html'))
  ? path.join(__dirname, 'public')
  : __dirname;

app.use(express.static(PUBLIC_DIR, {
  maxAge: '2h',
  setHeaders(res, filePath) {
    if (filePath.endsWith('sw.js')) res.setHeader('Cache-Control', 'no-cache');
  }
}));

app.get('/', (_req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    running,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
    scanning,
    timestamp: Date.now(),
    service: 'Whale Hunter Bitcoin Two Formula Only V17',
    websocketConnected: wsState.connected,
    publicHealthOnly: true
  });
});

app.get('/auth/me', (req, res) => {
  const user = getSessionUser(req);
  res.json({
    ok: true,
    authenticated: Boolean(user),
    role: user?.role || 'guest',
    canControl: user?.role === 'owner' || !REQUIRE_AUTH,
    canView: Boolean(user) || !REQUIRE_AUTH,
    requireAuth: REQUIRE_AUTH,
    timestamp: Date.now()
  });
});

app.post('/auth/login', (req, res) => {
  const pin = String(req.body?.pin || '').trim();
  let role = null;
  if (!REQUIRE_AUTH) role = 'owner';
  else if (safeEqual(pin, OWNER_PIN)) role = 'owner';
  else if (safeEqual(pin, VIEWER_PIN)) role = 'viewer';

  if (!role) {
    return res.status(401).json({ ok: false, error: 'BAD_PIN', message: 'Wrong access code.' });
  }

  const token = signSession({ role, iat: Date.now(), id: randomUUID() });
  res.setHeader('Set-Cookie', buildCookie(req, token));
  res.json({ ok: true, role, canControl: role === 'owner', timestamp: Date.now() });
});

app.post('/auth/logout', (req, res) => {
  res.setHeader('Set-Cookie', buildCookie(req, '', 0));
  res.json({ ok: true, timestamp: Date.now() });
});

app.get('/proof', requireViewer, (_req, res) => {
  res.json({ ok: true, proof: getProofSnapshot(), timestamp: Date.now() });
});

app.get('/admin/export', requireOwner, (_req, res) => {
  res.json({
    ok: true,
    exportedAt: Date.now(),
    config,
    arena: getArenaSnapshot(),
    hive: getHiveSnapshot(),
    proof: getProofSnapshot(),
    successfulFormulaLogs: hiveMemory.successFormulaLogs.slice().reverse(),
    lastScan,
    note: 'Paper-simulation data only. No real trades, no API keys.'
  });
});

app.post('/admin/reset-lab', requireOwner, (_req, res) => {
  resetArena();
  const proof = getProofSnapshot();
  broadcast('arena', getArenaSnapshot());
  broadcast('proof', proof);
  log('admin', 'Owner reset the mathematical lab and paper arena.');
  res.json({ ok: true, arena: getArenaSnapshot(), proof });
});

app.get('/arena', requireViewer, (_req, res) => {
  const arena = getArenaSnapshot();
  res.json({ ok: true, arena, botSwarm: arena.botSwarm, hive: arena.hive });
});

app.get('/hive', requireViewer, (_req, res) => {
  res.json({ ok: true, hive: getHiveSnapshot(), arena: getArenaSnapshot(), running, timestamp: Date.now() });
});

app.get('/formulas', requireViewer, (_req, res) => {
  res.json({
    ok: true,
    timestamp: Date.now(),
    formulaReports: hiveMemory.formulaReports.slice(-80).reverse(),
    consensus: getMathConsensus(),
    weights: getTopMathWeights(18),
    note: 'Live mathematical reports are paper-simulation learning only. No trading, no API keys.'
  });
});

app.get('/formula-success-logs', requireViewer, (req, res) => {
  const limit = clamp(Number(req.query.limit || 200), 1, SUCCESS_FORMULA_LOG_MAX);
  const symbol = String(req.query.symbol || '').trim().toUpperCase();
  const q = String(req.query.q || '').trim().toUpperCase();
  let logs = hiveMemory.successFormulaLogs.slice().reverse();
  if (symbol) logs = logs.filter(x => x.symbol === symbol);
  if (q) logs = logs.filter(x => JSON.stringify(x).toUpperCase().includes(q));
  logs = logs.slice(0, limit);
  res.json({
    ok: true,
    count: hiveMemory.successFormulaLogs.length,
    returned: logs.length,
    logs,
    text: logs.map(formatSuccessLogLine).join('\n'),
    note: 'Successful equation logs are paper-simulation records only. Copy/export them before a free host restarts if you need permanent history.',
    timestamp: Date.now()
  });
});

app.get('/formula-success-logs.txt', requireViewer, (req, res) => {
  const limit = clamp(Number(req.query.limit || 500), 1, SUCCESS_FORMULA_LOG_MAX);
  const symbol = String(req.query.symbol || '').trim().toUpperCase();
  const q = String(req.query.q || '').trim().toUpperCase();
  let logs = hiveMemory.successFormulaLogs.slice().reverse();
  if (symbol) logs = logs.filter(x => x.symbol === symbol);
  if (q) logs = logs.filter(x => JSON.stringify(x).toUpperCase().includes(q));
  logs = logs.slice(0, limit);
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.send(logs.map(formatSuccessLogBlock).join('\n\n---\n\n') || 'No successful equation logs yet.');
});

app.get('/state', requireViewer, (_req, res) => {
  res.json({
    ok: true,
    running,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
    serverSideAutonomous: true,
    pageIsOnlyViewer: true,
    scientistHiveV11: true,
    livingScientistLoop: true,
    tenMinuteLeverageSprint: true,
    quantProofV16: true,
    twoFormulaV17: true,
    successFormulaLogsV16: true,
    bitcoinOnlyV16: true,
    accessControl: true,
    minProofSamples: MIN_PROOF_SAMPLES,
    perfectAlertOnly: PERFECT_ALERT_ONLY,
    fakeLeverageOnly: true,
    collectiveMathLearning: true,
    timestamp: Date.now(),
    config,
    arena: getArenaSnapshot(),
    hive: getHiveSnapshot(),
    lastScan,
    websocket: {
      connected: wsState.connected,
      mode: wsState.mode,
      discoveredSymbols: wsState.discoveredSymbols.size,
      subscribedSymbols: wsState.subscribedSymbols.size,
      lastMessageAt: wsState.lastMessageAt
    }
  });
});

app.post('/arena/reset', requireOwner, (_req, res) => {
  resetArena();
  const arena = getArenaSnapshot();
  log('arena', 'Prediction Arena reset: 500 contestants are ready for a new tournament.');
  broadcast('arena', arena);
  res.json({ ok: true, arena });
});

app.get('/events', requireViewer, (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  const client = { id: randomUUID(), res };
  sseClients.add(client);
  sendSse(client, 'hello', {
    ok: true,
    running,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
    scanning,
    timestamp: Date.now(),
    config,
    arena: getArenaSnapshot(),
    hive: getHiveSnapshot(),
    lastScan,
    logs: logBuffer.slice(-30)
  });

  const heartbeat = setInterval(() => {
    sendSse(client, 'ping', { timestamp: Date.now(), running, scanning });
  }, 15000);

  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(client);
  });
});

app.post('/config', requireOwner, async (req, res) => {
  const incoming = req.body || {};
  const normalized = await normalizeConfig({ ...config, ...incoming });
  config = normalized.config;
  ensureTradeStreams(config.symbols);
  log('loadout', `Loadout saved: ${describeLoadout(config)}, whale $${formatUsd(config.whaleUsd)}, minScore ${config.minScore}, arena ${formatDuration(config.arenaHorizonSec)}.`);
  broadcast('config', { config, rejected: normalized.rejected, warnings: normalized.warnings, timestamp: Date.now() });
  if (running) scheduleNextScan(100);
  res.json({ ok: true, config, rejected: normalized.rejected, warnings: normalized.warnings });
});

app.post('/start', requireOwner, async (_req, res) => {
  if (!running) {
    running = true;
    ensureTradeStreams(config.symbols);
    scheduleNextScan(100);
    log('engine', `Hunt started. Scan interval: ${config.intervalSec}s. Arena horizon: ${formatDuration(config.arenaHorizonSec)}.`);
    broadcast('status', { running, scanning, timestamp: Date.now() });
  }
  res.json({ ok: true, running, config, arena: getArenaSnapshot() });
});

app.post('/stop', requireOwner, (_req, res) => {
  running = false;
  if (scanTimer) clearTimeout(scanTimer);
  scanTimer = null;
  log('engine', 'Hunt stopped. Radar is idle. Open arena predictions stay in memory until the server restarts or arena reset.');
  broadcast('status', { running, scanning, timestamp: Date.now() });
  res.json({ ok: true, running });
});

app.post('/scan', requireOwner, async (_req, res) => {
  const result = await runScan({ manual: true });
  res.status(result.ok ? 200 : 429).json(result);
});

app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ ok: false, error: 'SERVER_ERROR', message: err.message || String(err) });
});


function safeEqual(a, b) {
  const aa = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  if (aa.length !== bb.length) return false;
  return timingSafeEqual(aa, bb);
}

function parseCookies(req) {
  const header = req.headers.cookie || '';
  const out = {};
  for (const part of header.split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    const key = part.slice(0, i).trim();
    const val = part.slice(i + 1).trim();
    out[key] = decodeURIComponent(val);
  }
  return out;
}

function signSession(payload) {
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = createHmac('sha256', AUTH_SECRET).update(body).digest('base64url');
  return `${body}.${sig}`;
}

function verifySession(token) {
  if (!token || typeof token !== 'string' || !token.includes('.')) return null;
  const [body, sig] = token.split('.');
  const expected = createHmac('sha256', AUTH_SECRET).update(body).digest('base64url');
  if (!safeEqual(sig, expected)) return null;
  try {
    const payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    if (!payload || !['owner', 'viewer'].includes(payload.role)) return null;
    const ageMs = Date.now() - Number(payload.iat || 0);
    if (ageMs > 1000 * 60 * 60 * 24 * 14) return null;
    return payload;
  } catch {
    return null;
  }
}

function getSessionUser(req) {
  if (!REQUIRE_AUTH) return { role: 'owner', id: 'auth-disabled' };
  const cookies = parseCookies(req);
  return verifySession(cookies[AUTH_COOKIE]);
}

function buildCookie(req, value, maxAge = 60 * 60 * 24 * 14) {
  const secure = String(req.headers['x-forwarded-proto'] || '').includes('https') || req.secure;
  const parts = [`${AUTH_COOKIE}=${encodeURIComponent(value)}`, 'Path=/', 'HttpOnly', 'SameSite=Lax', `Max-Age=${maxAge}`];
  if (secure) parts.push('Secure');
  return parts.join('; ');
}

function requireViewer(req, res, next) {
  const user = getSessionUser(req);
  if (!user) return res.status(401).json({ ok: false, error: 'AUTH_REQUIRED', message: 'Login required.' });
  req.user = user;
  next();
}

function requireOwner(req, res, next) {
  const user = getSessionUser(req);
  if (!user) return res.status(401).json({ ok: false, error: 'AUTH_REQUIRED', message: 'Login required.' });
  if (user.role !== 'owner') return res.status(403).json({ ok: false, error: 'OWNER_ONLY', message: 'Owner permission required.' });
  req.user = user;
  next();
}

function scheduleNextScan(delayMs = config.intervalSec * 1000) {
  if (!running) return;
  if (scanTimer) clearTimeout(scanTimer);
  scanTimer = setTimeout(async () => {
    await runScan({ manual: false });
    scheduleNextScan(Math.max(3000, Number(config.intervalSec || 8) * 1000));
  }, delayMs);
}

async function runScan({ manual = false } = {}) {
  if (scanning) {
    const msg = 'Scan already running. Skipping overlapping request.';
    log('scanner', msg);
    return { ok: false, busy: true, message: msg };
  }

  scanning = true;
  const startedAt = Date.now();
  const cfg = structuredClone(config);
  const scanSymbols = getScanSymbols(cfg);
  const results = [];
  const errors = [];

  broadcast('scan-start', { timestamp: startedAt, manual, symbols: scanSymbols, allSymbolsMode: isAllSymbolsRequest(cfg.symbols) });
  log('scanner', `${manual ? 'Manual scan' : 'Auto scan'} launched for ${scanSymbols.length} sector(s)${isAllSymbolsRequest(cfg.symbols) ? ' in ALL BINANCE mode' : ''}.`);

  if (!scanSymbols.length) {
    scanning = false;
    const waiting = {
      ok: true,
      timestamp: Date.now(),
      durationMs: Date.now() - startedAt,
      manual,
      running,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
      config: cfg,
      results: [],
      hotCount: 0,
      top: null,
      arena: getArenaSnapshot(),
      errors: [],
      message: 'Waiting for Binance WebSocket symbol discovery.'
    };
    lastScan = waiting;
    log('scanner', 'Waiting for all-symbol WebSocket discovery. Try again in a few seconds.');
    broadcast('scan', waiting);
    broadcast('status', { running, scanning, timestamp: Date.now() });
    return waiting;
  }

  for (const symbol of scanSymbols) {
    try {
      const signal = await scanSymbol(symbol, cfg);
      results.push(signal);
      if (signal.price > 0) currentPrices.set(symbol, { price: signal.price, timestamp: signal.timestamp });
      broadcast('symbol', { signal, timestamp: Date.now() });
    } catch (error) {
      const safeError = {
        symbol,
        message: error.message || String(error),
        code: error.code || 'SCAN_ERROR'
      };
      errors.push(safeError);
      log('api-error', `${symbol}: ${safeError.message}`);
      broadcast('symbol-error', { ...safeError, timestamp: Date.now() });
    }
    await sleep(SYMBOL_DELAY_MS);
  }

  results.sort((a, b) => b.score - a.score || b.buyUsd - a.buyUsd);
  const hot = results.filter(item => item.hot);
  const top = results[0]?.symbol || null;
  updateCoinMemory(results, Date.now(), cfg);
  const arena = updateArena(results, cfg, Date.now());
  const finishedAt = Date.now();

  lastScan = {
    ok: true,
    timestamp: finishedAt,
    durationMs: finishedAt - startedAt,
    manual,
    running,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
    config: { ...cfg, activeSymbols: scanSymbols, activeSymbolCount: scanSymbols.length },
    results,
    hotCount: hot.length,
    top,
    arena,
    hive: getHiveSnapshot(),
    errors
  };

  scanning = false;
  log('scanner', `Scan complete: ${hot.length} hot signal(s), top target ${top || 'N/A'}, ${errors.length} API error(s). Arena open: ${arena.openCount}, resolved: ${arena.resolvedCount}.`);
  broadcast('scan', lastScan);
  broadcast('proof', getProofSnapshot());
  broadcast('arena', arena);
  broadcast('status', { running, scanning, timestamp: Date.now() });
  return lastScan;
}

async function scanSymbol(symbol, cfg) {
  const now = Date.now();
  const cutoff = now - Number(cfg.windowSec) * 1000;
  const buffer = tradeBuffers.get(symbol) || [];
  const recent = buffer
    .filter(t => Number(t.time) >= cutoff)
    .sort((a, b) => a.time - b.time);

  let buyUsd = 0;
  let sellUsd = 0;
  let bigBuyUsd = 0;
  let bigSellUsd = 0;
  let whaleTradeCount = 0;
  let high = -Infinity;
  let low = Infinity;

  for (const t of recent) {
    const usd = t.price * t.qty;
    high = Math.max(high, t.price);
    low = Math.min(low, t.price);

    if (t.buyerAggressive) {
      buyUsd += usd;
      if (usd >= cfg.whaleUsd) {
        bigBuyUsd += usd;
        whaleTradeCount += 1;
      }
    } else {
      sellUsd += usd;
      if (usd >= cfg.whaleUsd) {
        bigSellUsd += usd;
        whaleTradeCount += 1;
      }
    }
  }

  const totalFlow = buyUsd + sellUsd;
  const firstPrice = recent[0]?.price ?? currentPrices.get(symbol)?.price ?? 0;
  const lastPrice = recent.at(-1)?.price ?? currentPrices.get(symbol)?.price ?? 0;
  const buyPct = totalFlow > 0 ? buyUsd / totalFlow : 0;
  const netWhale = bigBuyUsd - bigSellUsd;
  const priceChangePct = firstPrice > 0 ? ((lastPrice - firstPrice) / firstPrice) * 100 : 0;
  const priceRangePct = low > 0 && Number.isFinite(high) ? ((high - low) / low) * 100 : 0;
  const absorption = totalFlow > cfg.whaleUsd * 3 && buyPct > 0.64 && priceRangePct < 0.75;

  let score = 0;
  const tags = [];

  if (totalFlow > cfg.whaleUsd * 3) { score += 18; tags.push('HIGH FLOW'); }
  if (bigBuyUsd > cfg.whaleUsd) { score += 20; tags.push('BIG BUY'); }
  if (buyPct > 0.64) { score += 20; tags.push('BUY IMBALANCE'); }
  if (netWhale > cfg.whaleUsd) { score += 15; tags.push('NET BUY'); }
  if (absorption) { score += 15; tags.push('ABSORPTION'); }
  if (priceChangePct > 0.2) { score += 10; tags.push('PRICE LIFT'); }
  if (bigSellUsd > bigBuyUsd * 1.2 && bigSellUsd > cfg.whaleUsd) { score -= 25; tags.push('SELL PRESSURE'); }

  score = clamp(Math.round(score), 0, 100);

  return {
    symbol,
    score,
    hot: score >= cfg.minScore,
    status: score >= cfg.minScore ? 'WHALE TARGET' : 'SCOUTING',
    price: lastPrice,
    buyUsd,
    sellUsd,
    totalFlow,
    buyPct,
    netWhale,
    priceChangePct,
    priceRangePct,
    bigBuyUsd,
    bigSellUsd,
    whaleTradeCount,
    tradeCount: recent.length,
    absorption,
    tags: recent.length ? tags : ['WAITING FOR STREAM'],
    explanation: explainSignalStory({ symbol, score, buyUsd, sellUsd, totalFlow, buyPct, netWhale, priceChangePct, priceRangePct, bigBuyUsd, bigSellUsd, whaleTradeCount, absorption, tags }),
    timestamp: Date.now(),
    note: score >= cfg.minScore ? 'possible whale footprint' : 'watch signal',
    source: 'Binance Futures WebSocket aggTrade stream'
  };
}


function getFixedFormulaByStyleKey(styleKey) {
  return FIXED_FORMULAS.find(f => f.styleKey === styleKey) || null;
}

function getFixedFormulaByDirection(direction) {
  return FIXED_FORMULAS.find(f => f.direction === direction) || null;
}

function getFixedFormulaContestant(styleKey) {
  return contestants.find(bot => bot.style?.key === styleKey) || null;
}

function fixedFormulaStatus() {
  return FIXED_FORMULAS.map(f => ({
    id: f.id,
    styleKey: f.styleKey,
    originalBot: f.originalBot,
    direction: f.direction,
    leverage: f.leverage,
    equation: f.equation,
    originalLine: f.originalLine
  }));
}

function updateArena(results, cfg, now) {
  const bySymbol = new Map(results.map(signal => [signal.symbol, signal]));
  const resolvedThisScan = [];
  const stillOpen = [];

  for (const prediction of activePredictions) {
    const priceState = currentPrices.get(prediction.symbol);
    if (now >= prediction.resolveAt && priceState?.price > 0) {
      const resolved = resolvePrediction(prediction, priceState.price, now, bySymbol.get(prediction.symbol));
      resolvedThisScan.push(resolved);
      resolvedPredictions.push(resolved);
    } else {
      stillOpen.push(prediction);
    }
  }

  activePredictions = stillOpen;
  if (resolvedPredictions.length > 2500) resolvedPredictions = resolvedPredictions.slice(-2500);

  const activeContestants = new Set(activePredictions.map(item => item.contestantId));
  const activeFormulaKeys = new Set(activePredictions.map(item => item.fixedFormulaId).filter(Boolean));
  const createdThisScan = [];

  if (Array.isArray(results) && results.length) {
    if (TWO_FORMULA_ONLY_MODE) {
      for (const fixedFormula of FIXED_FORMULAS) {
        if (activeFormulaKeys.has(fixedFormula.id)) continue;
        const contestant = getFixedFormulaContestant(fixedFormula.styleKey);
        if (!contestant || activeContestants.has(contestant.id)) continue;
        const prediction = createPrediction(contestant, results, cfg, now, fixedFormula);
        if (!prediction) continue;
        contestant.pending += 1;
        activePredictions.push(prediction);
        activeContestants.add(contestant.id);
        activeFormulaKeys.add(fixedFormula.id);
        createdThisScan.push(prediction);
      }
    } else {
      for (const contestant of contestants) {
        if (activeContestants.has(contestant.id)) continue;
        const prediction = createPrediction(contestant, results, cfg, now);
        if (!prediction) continue;
        contestant.pending += 1;
        activePredictions.push(prediction);
        activeContestants.add(contestant.id);
        createdThisScan.push(prediction);
      }
    }
  }

  if (createdThisScan.length) {
    log('arena', TWO_FORMULA_ONLY_MODE ? `Two fixed formulas opened ${createdThisScan.length} repeated paper trade(s) for ${formatDuration(cfg.arenaHorizonSec)}.` : `${createdThisScan.length} contestants locked new ${formatDuration(cfg.arenaHorizonSec)} watch predictions.`);
  }
  if (resolvedThisScan.length) {
    const wins = resolvedThisScan.filter(item => item.hit).length;
    log('arena', `${resolvedThisScan.length} arena prediction(s) resolved: ${wins} hit, ${resolvedThisScan.length - wins} missed.`);
  }

  return getArenaSnapshot({ createdThisScan, resolvedThisScan });
}

function createPrediction(contestant, results, cfg, now, fixedFormulaOverride = null) {
  let best = null;
  for (const signal of results) {
    if (!signal.price) continue;
    const reading = readSignal(contestant, signal, cfg);
    if (!best || reading.conviction > best.reading.conviction) best = { signal, reading };
  }
  if (!best) return null;

  const { signal, reading } = best;
  const fixedFormula = fixedFormulaOverride || reading.fixedFormula || getFixedFormulaByStyleKey(contestant.style?.key);
  const direction = fixedFormula?.direction || reading.direction;
  const prediction = {
    id: randomUUID(),
    contestantId: contestant.id,
    callsign: contestant.callsign,
    style: contestant.style.name,
    styleKey: contestant.style.key,
    styleAr: contestant.style.ar,
    indicator: contestant.style.indicator,
    symbol: signal.symbol,
    direction,
    entryPrice: signal.price,
    startTime: now,
    resolveAt: now + Number(cfg.arenaHorizonSec) * 1000,
    conviction: reading.conviction,
    basis: reading.basis,
    signalScore: signal.score,
    signalTags: signal.tags || [],
    signalSnapshot: signalFeatureSnapshot(signal),
    signalKey: makeSignalKeyFromSnapshot(signal.symbol, cfg.windowSec, direction, featureVectorFromSignal(signal), signal.timestamp || now),
    formula: reading.formula || null,
    hiveContext: reading.hiveContext || null,
    paperTrade: null,
    status: 'OPEN'
  };
  prediction.paperTrade = openPaperTrade(contestant, prediction, signal, now);
  const decisionReport = createDecisionFormulaReport(contestant, prediction, signal, reading, now);
  prediction.scientistReportId = decisionReport.id;
  pushScientistReport(decisionReport);
  contestant.learning.lastFormula = decisionReport.equation;
  contestant.lastPrediction = prediction;
  assignBotTarget(contestant, signal, prediction, now);
  return prediction;
}

function readSignal(contestant, signal, cfg) {
  const whaleUnit = Math.max(1, Number(cfg.whaleUsd) || defaultConfig.whaleUsd);
  const totalUnits = signal.totalFlow / whaleUnit;
  const buyUnits = signal.bigBuyUsd / whaleUnit;
  const sellUnits = signal.bigSellUsd / whaleUnit;
  const netUnits = signal.netWhale / whaleUnit;
  const buyPct = signal.buyPct * 100;
  const score = Number(signal.score) || 0;
  const range = Number(signal.priceRangePct) || 0;
  const lift = Number(signal.priceChangePct) || 0;
  const whaleCount = Number(signal.whaleTradeCount) || 0;
  const heavy = /^(BTC|ETH|SOL)USDT$/.test(signal.symbol) ? 1 : 0;
  const alt = heavy ? 0 : 1;
  const variant = contestant.variant;
  const fixedFormula = TWO_FORMULA_ONLY_MODE ? getFixedFormulaByStyleKey(contestant.style?.key) : null;

  let conviction = 25;
  let direction = fixedFormula?.direction || contestant.style.direction;
  let basis = fixedFormula?.entryNote || contestant.style.desc;

  if (fixedFormula) {
    // In Two Formula Only Mode, the game is not allowed to invent new strategies.
    // It repeatedly opens only the two user-approved BTCUSDT formula templates.
    const directionalPressure = fixedFormula.direction === 'UP'
      ? buyPct + Math.max(0, lift) * 18 + buyUnits * 5 - sellUnits * 2
      : (100 - buyPct) + Math.max(0, -lift) * 18 + sellUnits * 5 - buyUnits * 2;
    conviction = clamp(Math.round(62 + score * 0.18 + directionalPressure * 0.18), 55, 100);
    const formula = {
      equation: fixedFormula.originalLine,
      latex: fixedFormula.direction === 'UP'
        ? 'BTCUSDT\ UP:\ gross=2.0943\%-fees=0.9848\%-slippage=0.4924\%\Rightarrow net=0.6171\%'
        : 'BTCUSDT\ DOWN:\ gross=1.4970\%-fees=0.6394\%-slippage=0.3197\%\Rightarrow net=0.5379\%',
      raw: null,
      pressure: conviction,
      terms: [
        { feature: 'FIXED_FORMULA', w: 1, value: 1, product: 1 },
        { feature: fixedFormula.styleKey, w: 1, value: 1, product: 1 },
        { feature: 'BTCUSDT_ONLY', w: 1, value: 1, product: 1 }
      ],
      vector: featureVectorFromSignal(signal),
      baseStyle: fixedFormula.styleKey,
      finalConviction: conviction,
      finalDirection: fixedFormula.direction,
      fixedFormulaId: fixedFormula.id,
      fixedFormulaOriginalLine: fixedFormula.originalLine,
      fixedFormulaEquation: fixedFormula.equation,
      fixedLeverage: fixedFormula.leverage
    };
    return {
      conviction,
      direction: fixedFormula.direction,
      basis: `${fixedFormula.entryNote} Original equation locked حرفيًا.`,
      hiveContext: { boost: 0, note: 'fixed formula only' },
      mathContext: { boost: 0, note: 'no invented model' },
      formula,
      fixedFormula
    };
  }

  switch (contestant.style.key) {
    case 'TAKER_BUY':
      conviction = buyPct + score * 0.22 + buyUnits * 4 - sellUnits * 3;
      basis = `Buy% ${buyPct.toFixed(1)} with score ${score}`;
      direction = buyPct >= 58 ? 'UP' : 'WATCH';
      break;
    case 'BIG_BUY':
      conviction = 28 + buyUnits * 18 + score * 0.18 - sellUnits * 7;
      basis = `Big Buy ${formatUsd(signal.bigBuyUsd)} vs Big Sell ${formatUsd(signal.bigSellUsd)}`;
      direction = buyUnits > sellUnits ? 'UP' : 'WATCH';
      break;
    case 'NET_WHALE':
      conviction = 35 + netUnits * 16 + score * 0.20;
      basis = `Net Whale ${formatUsd(signal.netWhale)}`;
      direction = netUnits > 0.4 ? 'UP' : netUnits < -0.7 ? 'DOWN' : 'WATCH';
      break;
    case 'ABSORPTION':
      conviction = (signal.absorption ? 86 : 28) + Math.max(0, 0.75 - range) * 16 + Math.max(0, buyPct - 58) * 0.6;
      basis = signal.absorption ? 'Absorption: high buy flow + compressed range' : `Range ${range.toFixed(2)}%`;
      direction = signal.absorption ? 'UP' : 'WATCH';
      break;
    case 'PRICE_LIFT':
      conviction = 32 + Math.max(0, lift) * 24 + score * 0.35 + Math.max(0, buyPct - 50) * 0.35;
      basis = `Price lift ${lift.toFixed(2)}%`;
      direction = lift > 0.12 ? 'UP' : 'WATCH';
      break;
    case 'FLOW_BURST':
      conviction = 25 + totalUnits * 10 + score * 0.24 + Math.max(0, buyPct - 55) * 0.28;
      basis = `Total flow ${formatUsd(signal.totalFlow)}`;
      direction = totalUnits > 2.5 && buyPct > 54 ? 'UP' : 'WATCH';
      break;
    case 'WHALE_COUNT':
      conviction = 25 + whaleCount * 16 + buyUnits * 6 - sellUnits * 4 + score * 0.13;
      basis = `${whaleCount} whale-sized trade(s)`;
      direction = whaleCount > 0 && buyUnits >= sellUnits ? 'UP' : 'WATCH';
      break;
    case 'RANGE_LOCK':
      conviction = 25 + Math.max(0, 1.15 - range) * 28 + totalUnits * 6 + Math.max(0, buyPct - 58) * 0.45;
      basis = `Compressed range ${range.toFixed(2)}%`;
      direction = range < 0.75 && buyPct > 56 ? 'UP' : 'WATCH';
      break;
    case 'SELL_PRESSURE':
      conviction = 30 + sellUnits * 18 + Math.max(0, 50 - buyPct) * 0.8 - buyUnits * 7;
      basis = `Big Sell pressure ${formatUsd(signal.bigSellUsd)}`;
      direction = sellUnits > buyUnits * 1.15 && signal.bigSellUsd > whaleUnit ? 'DOWN' : 'WATCH';
      break;
    case 'TRAP_REVERSAL':
      conviction = 28 + sellUnits * 9 + Math.max(0, buyPct - 60) * 1.1 + Math.max(0, 0.8 - range) * 18;
      basis = `Possible sell trap: sell ${formatUsd(signal.bigSellUsd)}, buy% ${buyPct.toFixed(1)}`;
      direction = sellUnits > 0.5 && buyPct > 60 && range < 1.0 ? 'UP' : 'WATCH';
      break;
    case 'SCORE_PURITY':
      conviction = score;
      basis = `Pure score ${score}`;
      direction = score >= cfg.minScore ? 'UP' : 'WATCH';
      break;
    case 'LOW_RANGE_BOSS':
      conviction = score * 0.65 + Math.max(0, 0.7 - range) * 35;
      basis = `Boss-style low range ${range.toFixed(2)}%, score ${score}`;
      direction = score >= 80 && range < 0.75 ? 'UP' : 'WATCH';
      break;
    case 'DOGFIGHT':
      conviction = 72 - Math.abs(buyPct - 50) * 0.9 + totalUnits * 2;
      basis = `Buy/Sell dogfight near ${buyPct.toFixed(1)}% buy`;
      direction = Math.abs(buyPct - 50) < 9 ? 'WATCH' : buyPct > 59 ? 'UP' : 'DOWN';
      break;
    case 'MICRO_LIFT':
      conviction = 34 + Math.max(-0.1, lift) * 30 + Math.max(0, buyPct - 53) * 0.7 + score * 0.18;
      basis = `Micro lift ${lift.toFixed(2)}%, buy% ${buyPct.toFixed(1)}`;
      direction = lift > 0.05 && buyPct > 53 ? 'UP' : 'WATCH';
      break;
    case 'HEAVY_SYMBOL':
      conviction = 25 + heavy * 18 + totalUnits * 7 + score * 0.25 + Math.max(0, buyPct - 55) * 0.35;
      basis = heavy ? 'Heavy sector bias BTC/ETH/SOL' : 'Non-heavy sector, lower priority';
      direction = heavy && buyPct > 55 ? 'UP' : 'WATCH';
      break;
    case 'ALT_HUNTER':
      conviction = 25 + alt * 16 + Math.abs(lift) * 12 + score * 0.30 + totalUnits * 4;
      basis = alt ? 'Alt sector whale-flow chase' : 'Heavy symbol, lower alt bonus';
      direction = alt && buyPct > 57 ? 'UP' : 'WATCH';
      break;
    case 'SELL_FADE':
      conviction = 70 - Math.max(0, sellUnits - buyUnits) * 10 + Math.max(0, buyPct - 56) * 0.5 + Math.max(0, 1.0 - range) * 8;
      basis = `Sell fade filter, sell/buy whale ratio ${(sellUnits / Math.max(0.01, buyUnits)).toFixed(2)}`;
      direction = sellUnits > buyUnits * 1.35 ? 'WATCH' : buyPct > 58 ? 'UP' : 'WATCH';
      break;
    case 'IMBALANCE_PLUS':
      conviction = 24 + Math.max(0, buyPct - 50) * 1.1 + Math.max(0, netUnits) * 14 + score * 0.22;
      basis = `Buy imbalance ${buyPct.toFixed(1)}% + net whale ${formatUsd(signal.netWhale)}`;
      direction = buyPct > 61 && netUnits > 0 ? 'UP' : 'WATCH';
      break;
    case 'BOSS_ONLY':
      conviction = score >= 90 ? 95 + Math.min(5, buyUnits) : score * 0.55;
      basis = score >= 90 ? 'Boss-only gate opened' : 'Waiting for score 90+';
      direction = score >= 90 ? 'UP' : 'WATCH';
      break;
    case 'CALM_WATCHER':
    default:
      conviction = 76 - Math.max(0, score - 55) * 0.45 - Math.abs(lift) * 8;
      basis = `Calm watcher: score ${score}, lift ${lift.toFixed(2)}%`;
      direction = score < 55 ? 'WATCH' : buyPct > 62 ? 'UP' : 'WATCH';
      break;
  }

  conviction += variant * 2.2 + contestant.risk * 0.8;
  const hiveContext = getSharedHiveBoost(contestant, signal, direction);
  conviction += hiveContext.boost;
  if (hiveContext.note) basis += ` | Hive ${hiveContext.note}`;
  const mathContext = getCollectiveMathBoost(contestant, signal, direction);
  conviction += mathContext.boost;
  if (mathContext.note) basis += ` | Math ${mathContext.note}`;
  const formula = buildDecisionEquation(contestant, signal, { baseConviction: conviction, direction, hiveContext, mathContext });
  conviction = clamp(Math.round(conviction), 1, 100);
  if (conviction < contestant.minConviction) direction = 'WATCH';
  formula.finalConviction = conviction;
  formula.finalDirection = direction;
  return { conviction, direction, basis, hiveContext, mathContext, formula };
}

function computeWatchScore(prediction, movePct) {
  const threshold = prediction.direction === 'WATCH' ? 0.25 : 0.18;
  const directional = prediction.direction === 'UP'
    ? movePct >= threshold
    : prediction.direction === 'DOWN'
      ? movePct <= -threshold
      : Math.abs(movePct) <= threshold;
  return {
    type: 'WATCH_SCORE',
    directionalSignalWorked: directional,
    thresholdPct: threshold,
    movePct: Number(movePct.toFixed(4)),
    note: 'WATCH_SCORE measures signal behavior only. It is not TRADE_PROOF and never counts as a win.'
  };
}

function resolvePrediction(prediction, exitPrice, now, signal) {
  const contestant = contestants[prediction.contestantId - 1];
  const movePct = prediction.entryPrice > 0 ? ((exitPrice - prediction.entryPrice) / prediction.entryPrice) * 100 : 0;
  const watchScore = computeWatchScore(prediction, movePct);
  const paperResult = closePaperTrade(contestant, prediction, exitPrice, now);
  const tradeResult = paperResult?.result || (prediction.direction === 'WATCH' ? 'WATCH_ONLY' : 'NO_TRADE');
  const hit = tradeResult === 'WIN';
  const miss = tradeResult === 'LOSS';
  const neutral = tradeResult === 'NEUTRAL' || tradeResult === 'WATCH_ONLY' || tradeResult === 'NO_TRADE';
  const scoreDelta = hit
    ? 12 + Math.round(Math.min(34, Math.max(0, paperResult?.netPnlPct || 0) * 2.5)) + Math.round(prediction.conviction / 16)
    : miss
      ? -8 - Math.round(Math.min(18, Math.abs(paperResult?.netPnlPct || 0) * 1.6))
      : 0;
  const selfReview = analyzeOutcome(contestant, prediction, { hit, miss, neutral, tradeResult, watchScore, movePct, scoreDelta, paperResult }, signal);
  recordHiveOutcome(contestant, prediction, { hit, miss, neutral, tradeResult, watchScore, movePct, scoreDelta, paperResult, selfReview }, signal);

  contestant.xp = Math.max(0, contestant.xp + scoreDelta);
  contestant.pending = Math.max(0, contestant.pending - 1);
  contestant.games += 1;
  contestant.lastResult = hit ? 'WIN' : miss ? 'LOSS' : tradeResult;
  contestant.lastMovePct = movePct;
  contestant.lastSymbol = prediction.symbol;
  contestant.lastDirection = prediction.direction;
  contestant.lastResolvedAt = now;
  markBotResolved(contestant, hit, prediction, movePct, now, tradeResult);
  if (hit) {
    contestant.wins += 1;
    contestant.streak = Math.max(1, contestant.streak + 1);
  } else if (miss) {
    contestant.losses += 1;
    contestant.streak = Math.min(-1, contestant.streak - 1);
  } else {
    contestant.streak = 0;
  }
  contestant.bestStreak = Math.max(contestant.bestStreak, contestant.streak);

  return {
    ...prediction,
    status: 'RESOLVED',
    exitPrice,
    movePct,
    watchScore,
    hit,
    tradeResult,
    scoreDelta,
    paperResult,
    selfReview,
    proofCounted: Boolean(paperResult?.proofEligible && ['WIN', 'LOSS', 'NEUTRAL'].includes(tradeResult)),
    resolvedAt: now,
    resolvedSignalScore: signal?.score ?? null,
    resolvedSignalTags: signal?.tags ?? []
  };
}

function quantizeFeatureValue(value, decimals = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '0';
  return n.toFixed(decimals);
}

function makeSignalKeyFromSnapshot(symbol, timeframeSec, direction, vector = {}, timestamp = Date.now()) {
  const bucket = Math.floor(Number(timestamp || Date.now()) / Math.max(1000, Number(timeframeSec || 60) * 1000));
  const features = Object.keys(vector || {}).sort().map(k => `${k}:${quantizeFeatureValue(vector[k], 2)}`).join(';');
  return `${symbol || BTC_ONLY_SYMBOL}|${timeframeSec || 60}s|${direction || 'WATCH'}|${bucket}|${features}`;
}

function getPredictionSignalKey(prediction) {
  if (prediction.signalKey) return prediction.signalKey;
  const snap = prediction.signalSnapshot || {};
  return makeSignalKeyFromSnapshot(prediction.symbol, config.windowSec, prediction.direction, snap.vector || {}, prediction.startTime || Date.now());
}

function dedupePredictionsForDisplay(predictions = []) {
  const map = new Map();
  for (const pred of predictions) {
    const key = getPredictionSignalKey(pred);
    const existing = map.get(key);
    if (!existing) map.set(key, { ...pred, duplicateCount: 1 });
    else {
      existing.duplicateCount += 1;
      if ((pred.conviction || 0) > (existing.conviction || 0)) {
        map.set(key, { ...pred, duplicateCount: existing.duplicateCount });
      }
    }
  }
  return [...map.values()];
}

function rememberCountedTradeKey(key) {
  if (!key) return false;
  if (hiveMemory.countedTradeKeys.has(key)) return false;
  hiveMemory.countedTradeKeys.add(key);
  hiveMemory.countedTradeKeyQueue.push(key);
  const max = 6000;
  while (hiveMemory.countedTradeKeyQueue.length > max) {
    const old = hiveMemory.countedTradeKeyQueue.shift();
    hiveMemory.countedTradeKeys.delete(old);
  }
  return true;
}

function getArenaSnapshot(extra = {}) {
  const ranked = contestants
    .map(contestant => ({
      id: contestant.id,
      callsign: contestant.callsign,
      style: contestant.style.name,
      styleAr: contestant.style.ar,
      indicator: contestant.style.indicator,
      xp: contestant.xp,
      wins: contestant.wins,
      losses: contestant.losses,
      games: contestant.games,
      pending: contestant.pending,
      accuracy: contestant.games ? contestant.wins / contestant.games : 0,
      streak: contestant.streak,
      bestStreak: contestant.bestStreak,
      lastResult: contestant.lastResult,
      lastSymbol: contestant.lastSymbol,
      lastDirection: contestant.lastDirection,
      lastMovePct: contestant.lastMovePct,
      fakeCash: Number(contestant.cash.toFixed(2)),
      fakeEquity: Number(computeEquity(contestant).toFixed(2)),
      leverage: contestant.position?.leverage || 1,
      sprintVelocity: contestant.sprint?.velocityScore || 0,
      liquidations: contestant.sprint?.liquidations || 0,
      fakePnl: Number((computeEquity(contestant) - INITIAL_FAKE_USD).toFixed(2)),
      openPosition: contestant.position ? { symbol: contestant.position.symbol, side: contestant.position.side, entryPrice: contestant.position.entryPrice, notional: contestant.position.notional } : null,
      learningMood: contestant.learning?.mood || 'OBSERVING',
      lastAnalysis: contestant.learning?.lastAnalysis || null
    }))
    .sort((a, b) => b.xp - a.xp || b.accuracy - a.accuracy || b.wins - a.wins || a.id - b.id);

  const activeTop = dedupePredictionsForDisplay(activePredictions)
    .sort((a, b) => b.conviction - a.conviction || b.signalScore - a.signalScore)
    .slice(0, 20)
    .map(pred => ({
      ...pred,
      duplicateCount: pred.duplicateCount || 1,
      msLeft: Math.max(0, pred.resolveAt - Date.now())
    }));

  const recentResolved = resolvedPredictions.slice(-20).reverse();
  const styleBoard = buildStyleBoard();

  return {
    timestamp: Date.now(),
    horizonSec: config.arenaHorizonSec,
    totalContestants: contestants.length,
    openCount: activePredictions.length,
    resolvedCount: resolvedPredictions.length,
    leaders: ranked.slice(0, 20),
    activeTop,
    recentResolved,
    styleBoard,
    botSwarm: getBotSwarm(),
    hive: getHiveSnapshot(),
    createdThisScan: (extra.createdThisScan || []).slice(0, 20),
    resolvedThisScan: (extra.resolvedThisScan || []).slice(0, 20)
  };
}

function buildStyleBoard() {
  const groups = new Map();
  for (const contestant of contestants) {
    const key = contestant.style.key;
    const group = groups.get(key) || {
      key,
      style: contestant.style.name,
      styleAr: contestant.style.ar,
      indicator: contestant.style.indicator,
      racers: 0,
      xp: 0,
      wins: 0,
      games: 0,
      fakePnl: 0
    };
    group.racers += 1;
    group.xp += contestant.xp;
    group.wins += contestant.wins;
    group.games += contestant.games;
    group.fakePnl += computeEquity(contestant) - INITIAL_FAKE_USD;
    groups.set(key, group);
  }
  return [...groups.values()]
    .map(group => ({
      ...group,
      avgXp: group.racers ? group.xp / group.racers : 0,
      avgFakePnl: group.racers ? group.fakePnl / group.racers : 0,
      accuracy: group.games ? group.wins / group.games : 0
    }))
    .sort((a, b) => b.avgXp - a.avgXp || b.accuracy - a.accuracy)
    .slice(0, 10);
}

function resetArena() {
  activePredictions = [];
  resolvedPredictions = [];
  currentPrices = new Map();
  symbolSnapshots = new Map();
  lastLessonAt = new Map();
  resetHiveMemory();
  for (const contestant of contestants) {
    contestant.xp = 0;
    contestant.wins = 0;
    contestant.losses = 0;
    contestant.games = 0;
    contestant.pending = 0;
    contestant.streak = 0;
    contestant.bestStreak = 0;
    contestant.lastResult = null;
    contestant.lastMovePct = null;
    contestant.lastSymbol = null;
    contestant.lastDirection = null;
    contestant.lastPrediction = null;
    contestant.lastResolvedAt = null;
    contestant.cash = INITIAL_FAKE_USD;
    contestant.realizedPnl = 0;
    contestant.sprint = { targetMinutes: 10, maxLeverage: MAX_FAKE_LEVERAGE, objective: 'MAX_PROFIT_FASTEST', velocityScore: 0, liquidations: 0 };
    contestant.position = null;
    contestant.trades = 0;
    contestant.learning = createLearningState(contestant.style);
    resetBotState(contestant, Date.now());
  }
}

function createContestants(count) {
  const prefixes = ['Radar', 'Sonar', 'Neon', 'Abyss', 'Pulse', 'Tide', 'Hydra', 'Orbit', 'Cyber', 'Depth', 'Falcon', 'Kraken'];
  const arabic = ['الغواص', 'الصياد', 'المرصاد', 'النورس', 'القبطان', 'القناص', 'الحارس', 'الملاح', 'الرادار', 'الكشاف'];
  return Array.from({ length: count }, (_, idx) => {
    const id = idx + 1;
    const fixed = FIXED_FORMULAS[idx % FIXED_FORMULAS.length];
    const style = TWO_FORMULA_ONLY_MODE
      ? strategyDeck.find(item => item.key === fixed.styleKey) || strategyDeck[0]
      : strategyDeck[idx % strategyDeck.length];
    const prefix = prefixes[idx % prefixes.length];
    const ar = arabic[idx % arabic.length];
    return {
      id,
      callsign: `${prefix}-${String(id).padStart(3, '0')} ${ar}`,
      style,
      fixedFormulaId: TWO_FORMULA_ONLY_MODE ? fixed.id : null,
      variant: ((idx * 7) % 11) - 5,
      risk: ((idx * 13) % 17) - 8,
      minConviction: 42 + ((idx * 5) % 20),
      xp: 0,
      wins: 0,
      losses: 0,
      games: 0,
      pending: 0,
      streak: 0,
      bestStreak: 0,
      lastResult: null,
      lastMovePct: null,
      lastSymbol: null,
      lastDirection: null,
      lastPrediction: null,
      lastResolvedAt: null,
      cash: INITIAL_FAKE_USD,
      realizedPnl: 0,
      sprint: { targetMinutes: 10, maxLeverage: MAX_FAKE_LEVERAGE, objective: 'MAX_PROFIT_FASTEST', velocityScore: 0, liquidations: 0 },
      position: null,
      trades: 0,
      learning: createLearningState(style),
      bot: createBotState(id, style)
    };
  });
}

function createBotState(id, style) {
  const angle = ((id * 137.508) % 360) * Math.PI / 180;
  const radius = 10 + ((id * 17) % 36);
  const x = 50 + Math.cos(angle) * radius;
  const y = 50 + Math.sin(angle) * radius;
  return {
    x,
    y,
    targetX: x,
    targetY: y,
    heading: (angle * 180 / Math.PI + 90) % 360,
    speed: 5 + (id % 7) * 0.45,
    energy: 55 + (id % 41),
    mode: 'SCOUTING',
    stateTag: 'AUTO SCAN',
    targetSymbol: null,
    direction: style.direction || 'WATCH',
    lastDecisionAt: Date.now(),
    orbit: angle
  };
}



function startBotLoop() {
  if (botTimer) return;
  lastBotTickAt = Date.now();
  botTimer = setInterval(() => {
    const swarm = updateBotSwarm(Date.now());
    if (sseClients.size > 0) broadcast('bot-tick', swarm);
  }, BOT_TICK_MS);
}

function startLiveHiveLoop() {
  if (liveHiveTimer) return;
  liveHiveTimer = setInterval(() => {
    // Live mark-to-market and shared brain updates are server-side.
    // The browser is only a spectator; closing Safari does not stop this loop while the server remains awake.
    const payload = {
      timestamp: Date.now(),
      running,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
      hive: getHiveSnapshot(),
      arena: getArenaSnapshot(),
      botSwarm: getBotSwarm()
    };
    broadcast('hive-live', payload);
  }, LIVE_HIVE_PUSH_MS);
}


function startScientistThinkLoop() {
  if (scientistThinkTimer) return;
  scientistThinkTimer = setInterval(() => {
    try {
      const payload = generateLiveScientistThoughts(Date.now());
      if (payload && sseClients.size > 0) {
        broadcast('scientist-think', payload);
      }
    } catch (error) {
      log('scientist-error', `Live scientist loop error: ${error.message || error}`);
    }
  }, SCIENTIST_THINK_MS);
}

function generateLiveScientistThoughts(now) {
  if (!activePredictions.length) return null;
  const open = activePredictions
    .filter(pred => currentPrices.get(pred.symbol)?.price > 0)
    .sort((a, b) => (a.resolveAt - now) - (b.resolveAt - now));
  if (!open.length) return null;

  const reports = [];
  const stride = Math.max(1, Math.floor(open.length / LIVE_THINK_SAMPLE));
  const offset = Math.floor((now / SCIENTIST_THINK_MS) % stride);
  for (let i = offset; i < open.length && reports.length < LIVE_THINK_SAMPLE; i += stride) {
    const pred = open[i];
    const bot = contestants[pred.contestantId - 1];
    if (!bot) continue;
    const report = createLiveThoughtReport(bot, pred, now);
    if (!report) continue;
    reports.push(report);
    pushLiveThought(report);
    applyMicroLearning(report);
  }

  if (!reports.length) return null;
  const strongest = reports.slice().sort((a, b) => Math.abs(b.liveReward) - Math.abs(a.liveReward))[0];
  hiveMemory.consensus = {
    timestamp: now,
    version: hiveMemory.collectiveVersion,
    source: strongest.callsign,
    symbol: strongest.symbol,
    result: strongest.liveReward >= 0 ? 'LIVE_CONFIRMING' : 'LIVE_WARNING',
    equation: 'C_live = C₀ + Σ(w·x) + markToMarket(PnL)',
    teaching: `نبض حي من ${strongest.callsign}: ${strongest.symbol} ${strongest.liveReward >= 0 ? 'يؤكد الفرضية' : 'يحذر من الفرضية'}، والخلية تعدل الأوزان قبل نهاية الجولة.`
  };
  return { timestamp: now, reports, hive: getHiveSnapshot(), arena: getArenaSnapshot(), consensus: hiveMemory.consensus };
}

function createLiveThoughtReport(contestant, prediction, now) {
  const price = currentPrices.get(prediction.symbol)?.price;
  if (!price || !prediction.entryPrice) return null;
  const side = prediction.direction === 'DOWN' ? -1 : prediction.direction === 'UP' ? 1 : 0;
  const movePct = ((price - prediction.entryPrice) / prediction.entryPrice) * 100;
  const signedMove = side * movePct;
  const pos = contestant.position;
  const grossPnl = pos && pos.symbol === prediction.symbol
    ? (price - pos.entryPrice) * pos.qty * (pos.side === 'LONG' ? 1 : -1)
    : 0;
  const vector = prediction.signalSnapshot?.vector || featureVectorFromSignal(prediction.signalSnapshot || {});
  const liveReward = clamp(signedMove * 2 + grossPnl / 8, -2.5, 2.5);
  const terms = Object.entries(vector).map(([feature, value]) => ({
    feature,
    value: Number(Number(value || 0).toFixed(4)),
    gradient: Number((liveReward * Number(value || 0)).toFixed(4))
  })).sort((a, b) => Math.abs(b.gradient) - Math.abs(a.gradient)).slice(0, 5);
  const mood = liveReward > 0.18 ? 'CONFIRMING' : liveReward < -0.18 ? 'QUESTIONING' : 'OBSERVING';
  contestant.learning.mood = mood;
  contestant.learning.lastAnalysis = liveReward >= 0
    ? `الفكرة تعمل مؤقتًا على ${prediction.symbol}: الحركة الحالية ${signedMove.toFixed(3)}% مع PnL وهمي ${grossPnl.toFixed(2)}.`
    : `الفكرة تحت الاختبار على ${prediction.symbol}: الحركة ضد الفرضية ${signedMove.toFixed(3)}%، سأخفض الثقة إن استمر العكس.`;
  return {
    id: randomUUID(),
    kind: 'LIVE_THOUGHT',
    timestamp: now,
    phase: 'نبض تفكير حي',
    contestantId: contestant.id,
    callsign: contestant.callsign,
    style: contestant.style.name,
    styleAr: contestant.style.ar,
    symbol: prediction.symbol,
    direction: prediction.direction,
    conviction: prediction.conviction,
    equation: `V = PnL%/min + Lev(${pos?.leverage || 1}x) + sign(dir)·Δprice = ${liveReward.toFixed(3)}; Δwᵢ = μ·V·xᵢ`,
    latex: `V=\frac{PnL\%}{min}+Lev+sign(dir)\Delta price`,
    terms,
    vector,
    liveReward: Number(liveReward.toFixed(4)),
    result: liveReward >= 0 ? 'LIVE_OK' : 'LIVE_BAD',
    pnl: Number(grossPnl.toFixed(4)),
    leverage: pos?.leverage || 1,
    liquidationPrice: pos?.liquidationPrice || null,
    movePct: Number(movePct.toFixed(4)),
    signedMove: Number(signedMove.toFixed(4)),
    msLeft: Math.max(0, prediction.resolveAt - now),
    message: `${contestant.callsign} يحسب سباق 10 دقائق على ${prediction.symbol}: V=${liveReward.toFixed(3)} ورافعة ${pos?.leverage || 1}x قبل الإغلاق`,
    teaching: liveReward >= 0
      ? `الخلية ترفع قليلًا وزن الحدود التي تؤكد الفرضية قبل انتهاء الصفقة الوهمية.`
      : `الخلية تخفض قليلًا وزن الحدود التي بدأت تخدع الروبوت قبل انتهاء الصفقة الوهمية.`
  };
}

function pushLiveThought(report) {
  hiveMemory.totalLiveThoughts += 1;
  pushLimited(hiveMemory.liveThoughts, report, 220);
  pushLimited(hiveMemory.formulaReports, report, HIVE_REPORT_MAX);
  pushLimited(hiveMemory.scientistDebates, {
    id: report.id,
    timestamp: report.timestamp,
    speaker: report.callsign,
    symbol: report.symbol,
    result: report.result,
    equation: report.equation,
    teaching: report.teaching
  }, 220);
  hiveMemory.totalFormulaReports += 1;
}

function applyMicroLearning(report) {
  if (!report?.terms?.length || !Number.isFinite(Number(report.liveReward))) return;
  const changed = [];
  const reward = Number(report.liveReward);
  for (const term of report.terms) {
    const delta = clamp(MICRO_LEARN_RATE * reward * Number(term.value || 0), -0.012, 0.012);
    if (Math.abs(delta) < 0.0001) continue;
    const oldWeight = Number(hiveMemory.mathWeights.get(term.feature) || 0);
    const next = clamp(oldWeight + delta, -0.55, 0.55);
    hiveMemory.mathWeights.set(term.feature, Number(next.toFixed(5)));
    const st = hiveMemory.mathStats.get(term.feature) || { feature: term.feature, updates: 0, up: 0, down: 0, netReward: 0, lastDelta: 0 };
    st.updates += 1;
    if (delta >= 0) st.up += 1; else st.down += 1;
    st.netReward += reward;
    st.lastDelta = Number(delta.toFixed(5));
    hiveMemory.mathStats.set(term.feature, st);
    changed.push({ feature: term.feature, delta: Number(delta.toFixed(5)), weight: Number(next.toFixed(5)) });
  }
  if (changed.length) {
    hiveMemory.collectiveVersion += 1;
    for (const bot of contestants) {
      bot.learning.learnedFrom += 1;
      for (const change of changed) {
        const old = Number(bot.learning.learnedWeights[change.feature] || 0);
        bot.learning.learnedWeights[change.feature] = Number(clamp(old + change.delta * 0.08, -0.22, 0.22).toFixed(5));
      }
    }
  }
}

function updateBotSwarm(now) {
  const dt = clamp((now - lastBotTickAt) / 1000, 0.2, 2.25);
  lastBotTickAt = now;

  for (const contestant of contestants) {
    const bot = contestant.bot || (contestant.bot = createBotState(contestant.id, contestant.style));
    const hasOpen = activePredictions.some(pred => pred.contestantId === contestant.id);

    if (!hasOpen && now - bot.lastDecisionAt > 1800 + (contestant.id % 9) * 210) {
      setPatrolTarget(contestant, now);
    }

    const dx = bot.targetX - bot.x;
    const dy = bot.targetY - bot.y;
    const dist = Math.hypot(dx, dy) || 0.001;
    const baseSpeed = bot.speed + Math.max(-1.4, Math.min(2.2, contestant.risk / 5));
    const convictionBoost = contestant.lastPrediction ? contestant.lastPrediction.conviction / 85 : 0.55;
    const modeBoost = bot.mode === 'CHASING' ? 1.35 : bot.mode === 'RED_TIDE' ? 1.15 : bot.mode === 'CELEBRATE' ? 1.55 : 0.85;
    const step = Math.min(dist, baseSpeed * convictionBoost * modeBoost * dt);

    const angle = Math.atan2(dy, dx);
    const orbitForce = (bot.mode === 'ORBIT' || bot.mode === 'SCOUTING') ? Math.sin((now / 700) + contestant.id) * 0.45 : Math.sin((now / 420) + contestant.id) * 0.12;
    bot.x += Math.cos(angle) * step + Math.cos(angle + Math.PI / 2) * orbitForce;
    bot.y += Math.sin(angle) * step + Math.sin(angle + Math.PI / 2) * orbitForce;
    bot.heading = (Math.atan2(dy, dx) * 180 / Math.PI + 90 + 360) % 360;
    bot.energy = clamp(bot.energy + (bot.mode === 'CHASING' ? -0.14 : 0.08), 18, 100);

    keepBotInsideRadar(bot);

    if (dist < 2.2) {
      if (bot.mode === 'CHASING' || bot.mode === 'RED_TIDE') {
        jitterAroundTarget(contestant, now);
      } else {
        setPatrolTarget(contestant, now);
      }
    }
  }

  return getBotSwarm();
}

function assignBotTarget(contestant, signal, prediction, now) {
  const bot = contestant.bot || (contestant.bot = createBotState(contestant.id, contestant.style));
  const base = symbolRadarPosition(signal.symbol);
  const spreadAngle = ((contestant.id * 29) % 360) * Math.PI / 180;
  const spread = 2.5 + (contestant.id % 11) * 0.42;
  bot.targetX = base.x + Math.cos(spreadAngle) * spread;
  bot.targetY = base.y + Math.sin(spreadAngle) * spread;
  bot.targetSymbol = signal.symbol;
  bot.direction = prediction.direction;
  bot.lastDecisionAt = now;
  bot.energy = clamp(62 + prediction.conviction * 0.38 + contestant.risk * 0.6, 20, 100);

  if (prediction.direction === 'UP') {
    bot.mode = signal.score >= 90 ? 'BOSS_RUSH' : 'CHASING';
    bot.stateTag = signal.score >= 90 ? 'BOSS RUSH' : 'CHASE BUY';
  } else if (prediction.direction === 'DOWN') {
    bot.mode = 'RED_TIDE';
    bot.stateTag = 'SELL TRACK';
  } else {
    bot.mode = 'ORBIT';
    bot.stateTag = 'WATCH ORBIT';
  }
  keepBotInsideRadar(bot);
}

function jitterAroundTarget(contestant, now) {
  const bot = contestant.bot;
  if (!bot?.targetSymbol) return setPatrolTarget(contestant, now);
  const base = symbolRadarPosition(bot.targetSymbol);
  const angle = ((contestant.id * 41 + Math.floor(now / 1000) * 17) % 360) * Math.PI / 180;
  const radius = bot.mode === 'ORBIT' ? 8 + (contestant.id % 10) : 3 + (contestant.id % 6);
  bot.targetX = base.x + Math.cos(angle) * radius;
  bot.targetY = base.y + Math.sin(angle) * radius;
  keepBotInsideRadar(bot);
}

function setPatrolTarget(contestant, now) {
  const bot = contestant.bot || (contestant.bot = createBotState(contestant.id, contestant.style));
  const lane = contestant.id % 20;
  const angle = ((contestant.id * 47 + Math.floor(now / 1500) * (7 + lane % 5)) % 360) * Math.PI / 180;
  const radius = 12 + ((contestant.id * 19 + Math.floor(now / 3000)) % 34);
  bot.targetX = 50 + Math.cos(angle) * radius;
  bot.targetY = 50 + Math.sin(angle) * radius;
  bot.mode = contestant.lastResult === 'HIT' ? 'CELEBRATE' : contestant.lastResult === 'MISS' ? 'RECALIBRATE' : 'SCOUTING';
  bot.stateTag = contestant.lastResult === 'HIT' ? 'XP BOOST' : contestant.lastResult === 'MISS' ? 'RECALIBRATE' : 'AUTO PATROL';
  bot.targetSymbol = contestant.lastSymbol || null;
  bot.direction = contestant.lastDirection || contestant.style.direction || 'WATCH';
  bot.lastDecisionAt = now;
  keepBotInsideRadar(bot);
}

function markBotResolved(contestant, hit, prediction, movePct, now, tradeResult = null) {
  const bot = contestant.bot || (contestant.bot = createBotState(contestant.id, contestant.style));
  bot.mode = hit ? 'CELEBRATE' : tradeResult === 'NEUTRAL' ? 'ANALYZING' : tradeResult === 'WATCH_ONLY' ? 'ORBIT' : 'RECALIBRATE';
  bot.stateTag = hit ? `WIN ${movePct.toFixed(2)}%` : tradeResult === 'NEUTRAL' ? `NEUTRAL ${movePct.toFixed(2)}%` : tradeResult === 'WATCH_ONLY' ? 'WATCH ONLY' : `LOSS ${movePct.toFixed(2)}%`;
  bot.targetSymbol = prediction.symbol;
  bot.direction = prediction.direction;
  bot.energy = clamp(bot.energy + (hit ? 18 : tradeResult === 'NEUTRAL' || tradeResult === 'WATCH_ONLY' ? 0 : -13), 18, 100);
  const base = symbolRadarPosition(prediction.symbol);
  const angle = ((contestant.id * 23 + now / 1000) % 360) * Math.PI / 180;
  const radius = hit ? 13 + (contestant.id % 9) : 20 + (contestant.id % 13);
  bot.targetX = base.x + Math.cos(angle) * radius;
  bot.targetY = base.y + Math.sin(angle) * radius;
  bot.lastDecisionAt = now;
  keepBotInsideRadar(bot);
}

function resetBotState(contestant, now) {
  contestant.bot = createBotState(contestant.id, contestant.style);
  setPatrolTarget(contestant, now);
}

function keepBotInsideRadar(bot) {
  const dx = bot.x - 50;
  const dy = bot.y - 50;
  const radius = Math.hypot(dx, dy);
  const maxRadius = 47;
  if (radius > maxRadius) {
    bot.x = 50 + (dx / radius) * maxRadius;
    bot.y = 50 + (dy / radius) * maxRadius;
  }

  const tdx = bot.targetX - 50;
  const tdy = bot.targetY - 50;
  const targetRadius = Math.hypot(tdx, tdy);
  if (targetRadius > maxRadius) {
    bot.targetX = 50 + (tdx / targetRadius) * maxRadius;
    bot.targetY = 50 + (tdy / targetRadius) * maxRadius;
  }
}

function symbolRadarPosition(symbol) {
  let hash = 0;
  for (let i = 0; i < symbol.length; i++) hash = ((hash << 5) - hash + symbol.charCodeAt(i)) | 0;
  const a = Math.abs(hash);
  const angle = (a % 360) * Math.PI / 180;
  const radius = 18 + (a % 54);
  return {
    x: 50 + Math.cos(angle) * radius * 0.92,
    y: 50 + Math.sin(angle) * radius * 0.92
  };
}

function getBotSwarm() {
  let chasing = 0;
  let scouting = 0;
  let redTide = 0;
  let watch = 0;
  let bossRush = 0;
  const rankedIds = new Map(contestants
    .slice()
    .sort((a, b) => b.xp - a.xp || b.wins - a.wins || a.id - b.id)
    .slice(0, 12)
    .map((c, index) => [c.id, index + 1]));

  const bots = contestants.map(contestant => {
    const bot = contestant.bot || (contestant.bot = createBotState(contestant.id, contestant.style));
    if (bot.mode === 'CHASING') chasing += 1;
    else if (bot.mode === 'RED_TIDE') redTide += 1;
    else if (bot.mode === 'ORBIT') watch += 1;
    else if (bot.mode === 'BOSS_RUSH') bossRush += 1;
    else scouting += 1;

    return {
      id: contestant.id,
      callsign: contestant.callsign,
      styleAr: contestant.style.ar,
      indicator: contestant.style.indicator,
      x: Number(bot.x.toFixed(2)),
      y: Number(bot.y.toFixed(2)),
      heading: Number(bot.heading.toFixed(1)),
      mode: bot.mode,
      stateTag: bot.stateTag,
      targetSymbol: bot.targetSymbol,
      direction: bot.direction,
      energy: Math.round(bot.energy),
      xp: contestant.xp,
      rank: rankedIds.get(contestant.id) || null
    };
  });

  return {
    timestamp: Date.now(),
    total: contestants.length,
    moving: contestants.length,
    chasing,
    scouting,
    redTide,
    watch,
    bossRush,
    bots
  };
}


function createLearningState(style) {
  return {
    confidence: 50,
    mood: 'OBSERVING',
    repeatRule: null,
    avoidRule: null,
    lastAnalysis: null,
    winsByTag: {},
    lossesByTag: {},
    learnedWeights: {},
    learnedFrom: 0,
    lastFormula: null,
    styleKey: style?.key || 'UNKNOWN'
  };
}

function createHiveMemory() {
  const successFormulaLogs = [];
  return {
    startedAt: Date.now(),
    lessons: [],
    reviews: [],
    broadcasts: [],
    formulaReports: [],
    // Keep both names as aliases so old/new dashboard code cannot crash.
    successFormulaLogs,
    successfulFormulaLogs: successFormulaLogs,
    scientistDebates: [],
    liveThoughts: [],
    mathWeights: new Map(),
    mathStats: new Map(),
    consensus: null,
    totalFormulaReports: 0,
    totalLiveThoughts: 0,
    collectiveVersion: 0,
    patternStats: new Map(),
    symbolStats: new Map(),
    totalResolved: 0,
    totalPaperTrades: 0,
    countedTradeKeys: new Set(),
    countedTradeKeyQueue: []
  };
}

function resetHiveMemory() {
  hiveMemory.startedAt = Date.now();
  hiveMemory.lessons = [];
  hiveMemory.reviews = [];
  hiveMemory.broadcasts = [];
  hiveMemory.formulaReports = [];
  const successFormulaLogs = [];
  hiveMemory.successFormulaLogs = successFormulaLogs;
  hiveMemory.successfulFormulaLogs = successFormulaLogs;
  hiveMemory.scientistDebates = [];
  hiveMemory.liveThoughts = [];
  hiveMemory.mathWeights = new Map();
  hiveMemory.mathStats = new Map();
  hiveMemory.consensus = null;
  hiveMemory.totalFormulaReports = 0;
  hiveMemory.totalLiveThoughts = 0;
  hiveMemory.collectiveVersion = 0;
  hiveMemory.patternStats = new Map();
  hiveMemory.symbolStats = new Map();
  hiveMemory.totalResolved = 0;
  hiveMemory.totalPaperTrades = 0;
  hiveMemory.countedTradeKeys = new Set();
  hiveMemory.countedTradeKeyQueue = [];
}

function signalFeatureSnapshot(signal) {
  return {
    symbol: signal.symbol,
    price: signal.price,
    score: signal.score,
    buyPct: signal.buyPct,
    totalFlow: signal.totalFlow,
    bigBuyUsd: signal.bigBuyUsd,
    bigSellUsd: signal.bigSellUsd,
    netWhale: signal.netWhale,
    priceChangePct: signal.priceChangePct,
    priceRangePct: signal.priceRangePct,
    whaleTradeCount: signal.whaleTradeCount,
    absorption: signal.absorption,
    tags: [...(signal.tags || [])],
    explanation: signal.explanation || null,
    vector: featureVectorFromSignal(signal),
    timestamp: signal.timestamp || Date.now()
  };
}

function featureLabelsFromSignal(signal) {
  const tags = new Set(signal.tags || []);
  const buyPct = Number(signal.buyPct || 0) * 100;
  if (buyPct >= 64) tags.add('BUYERS_DOMINANT');
  if (buyPct <= 42) tags.add('SELLERS_DOMINANT');
  if (signal.bigBuyUsd > config.whaleUsd) tags.add('BIG_BUY_PRINT');
  if (signal.bigSellUsd > config.whaleUsd) tags.add('BIG_SELL_PRINT');
  if (signal.netWhale > config.whaleUsd) tags.add('NET_WHALE_BUY');
  if (signal.netWhale < -config.whaleUsd) tags.add('NET_WHALE_SELL');
  if (Math.abs(signal.priceChangePct || 0) > 0.2) tags.add(signal.priceChangePct > 0 ? 'PRICE_LIFT_SEEN' : 'PRICE_DROP_SEEN');
  if ((signal.priceRangePct || 0) < 0.75 && (signal.totalFlow || 0) > config.whaleUsd * 2) tags.add('COMPRESSED_FLOW');
  if (signal.absorption) tags.add('ABSORPTION_PATTERN');
  if (!tags.size) tags.add('NO_CLEAR_PATTERN');
  return [...tags].slice(0, 12);
}

function explainSignalStory(signal) {
  const buyPct = Number(signal.buyPct || 0) * 100;
  const cluesUp = [];
  const cluesDown = [];
  const neutral = [];

  if (signal.totalFlow > config.whaleUsd * 3) neutral.push(`تدفق عالي ${formatUsd(signal.totalFlow)}`);
  if (signal.bigBuyUsd > config.whaleUsd) cluesUp.push(`ظهرت صفقة/صفقات شراء كبيرة ${formatUsd(signal.bigBuyUsd)}`);
  if (buyPct > 64) cluesUp.push(`اختلال شراء واضح: ${buyPct.toFixed(1)}% من التدفق شراء عدواني`);
  if (signal.netWhale > config.whaleUsd) cluesUp.push(`صافي الحيتان شراء ${formatUsd(signal.netWhale)}`);
  if (signal.absorption) cluesUp.push('امتصاص: شراء قوي مع نطاق سعري مضغوط');
  if (signal.priceChangePct > 0.2) cluesUp.push(`رفع سعري داخل النافذة ${signal.priceChangePct.toFixed(2)}%`);

  if (signal.bigSellUsd > config.whaleUsd) cluesDown.push(`ظهرت صفقة/صفقات بيع كبيرة ${formatUsd(signal.bigSellUsd)}`);
  if (buyPct < 42 && signal.totalFlow > config.whaleUsd) cluesDown.push(`اختلال بيع: المشترين فقط ${buyPct.toFixed(1)}%`);
  if (signal.netWhale < -config.whaleUsd) cluesDown.push(`صافي الحيتان بيع ${formatUsd(signal.netWhale)}`);
  if (signal.priceChangePct < -0.2) cluesDown.push(`ضغط نزول داخل النافذة ${signal.priceChangePct.toFixed(2)}%`);
  if (signal.priceRangePct > 1.2 && signal.bigSellUsd > signal.bigBuyUsd) cluesDown.push('النطاق توسّع مع بيع أكبر من الشراء');

  const direction = signal.priceChangePct > 0.2 ? 'UP' : signal.priceChangePct < -0.2 ? 'DOWN' : 'WATCH';
  return {
    direction,
    whyUp: cluesUp.slice(0, 5),
    whyDown: cluesDown.slice(0, 5),
    neutral: neutral.slice(0, 4),
    beforeRise: cluesUp.length ? cluesUp.slice(0, 4) : ['لم تظهر بصمة صعود كافية بعد'],
    beforeDrop: cluesDown.length ? cluesDown.slice(0, 4) : ['لم تظهر بصمة نزول كافية بعد'],
    summary: direction === 'UP'
      ? `ارتفاع/رفع محتمل لأن ${cluesUp[0] || 'التدفق يميل للمشترين'}`
      : direction === 'DOWN'
        ? `نزول/ضغط محتمل لأن ${cluesDown[0] || 'التدفق يميل للبائعين'}`
        : 'قراءة مراقبة: لا يوجد سبب غالب واضح حتى الآن'
  };
}

function updateCoinMemory(results, now, cfg) {
  const lookback = Math.max(60000, Number(cfg.windowSec || 60) * 1000 * 3);
  for (const signal of results) {
    if (!signal?.symbol || !signal.price) continue;
    const arr = symbolSnapshots.get(signal.symbol) || [];
    let older = null;
    for (let i = arr.length - 1; i >= 0; i--) {
      if (now - arr[i].timestamp >= lookback) { older = arr[i]; break; }
    }
    if (older?.price > 0) {
      const movePct = ((signal.price - older.price) / older.price) * 100;
      const lastAt = lastLessonAt.get(signal.symbol) || 0;
      if (Math.abs(movePct) >= 0.28 && now - lastAt > 90000) {
        const direction = movePct > 0 ? 'UP' : 'DOWN';
        const lesson = buildCoinLesson(signal.symbol, direction, movePct, older, signal, now);
        pushLimited(hiveMemory.lessons, lesson, 120);
        updateSymbolBrain(signal.symbol, direction, older, movePct);
        lastLessonAt.set(signal.symbol, now);
        log('hive', `${signal.symbol} learned ${direction}: ${lesson.short}`);
      }
    }

    arr.push({
      timestamp: now,
      price: signal.price,
      score: signal.score,
      buyPct: signal.buyPct,
      tags: featureLabelsFromSignal(signal),
      snapshot: signalFeatureSnapshot(signal)
    });
    const keepAfter = now - Math.max(3600000, Number(cfg.arenaHorizonSec || 3600) * 1000);
    symbolSnapshots.set(signal.symbol, arr.filter(x => x.timestamp >= keepAfter).slice(-360));
  }
}

function buildCoinLesson(symbol, direction, movePct, before, after, now) {
  const beforeTags = before.tags || [];
  const main = beforeTags.slice(0, 4).join(' + ') || 'NO_CLEAR_PATTERN';
  const short = `${main} ظهر قبل ${direction === 'UP' ? 'صعود' : 'نزول'} ${movePct.toFixed(2)}%`;
  return {
    id: randomUUID(),
    timestamp: now,
    symbol,
    direction,
    movePct,
    beforeTags,
    beforeScore: before.score,
    afterScore: after.score,
    beforePrice: before.price,
    afterPrice: after.price,
    short,
    ar: direction === 'UP'
      ? `${symbol}: قبل الصعود كانت العلامات ${main}. الخلية تحفظها كاحتمال تكرار.`
      : `${symbol}: قبل النزول كانت العلامات ${main}. الخلية تحفظها كتحذير هبوط.`
  };
}

function updateSymbolBrain(symbol, direction, before, movePct) {
  const brain = hiveMemory.symbolStats.get(symbol) || {
    symbol,
    upMoves: 0,
    downMoves: 0,
    factors: {},
    lastLesson: null,
    netMoveLearned: 0
  };
  if (direction === 'UP') brain.upMoves += 1; else brain.downMoves += 1;
  brain.netMoveLearned += movePct;
  for (const tag of before.tags || []) {
    brain.factors[tag] = (brain.factors[tag] || 0) + (direction === 'UP' ? 1 : -1);
  }
  brain.lastLesson = Date.now();
  hiveMemory.symbolStats.set(symbol, brain);
}

function chooseFakeLeverage(contestant, prediction, signal) {
  const fixedFormula = getFixedFormulaByStyleKey(contestant.style?.key) || getFixedFormulaByDirection(prediction.direction);
  if (TWO_FORMULA_ONLY_MODE && fixedFormula) return fixedFormula.leverage;
  if (!['UP', 'DOWN'].includes(prediction.direction)) return 1;
  const maxLev = Math.max(1, Number(config.maxFakeLeverage || MAX_FAKE_LEVERAGE));
  const conviction = Number(prediction.conviction || 0);
  const score = Number(signal?.score || prediction.signalScore || 0);
  const range = Number(signal?.priceRangePct || 0);
  const buyPct = Number(signal?.buyPct || 0) * 100;
  const riskBias = Number(contestant.risk || 0);
  let lev = 1;
  lev += Math.max(0, conviction - 52) / 48 * maxLev * 0.42;
  lev += Math.max(0, score - 65) / 35 * maxLev * 0.22;
  lev += Math.max(0, riskBias) / 8 * maxLev * 0.10;
  if ((signal?.tags || []).includes('BOSS WHALE') || score >= 90) lev += maxLev * 0.18;
  if (range > 1.5) lev -= maxLev * 0.18;
  if ((signal?.tags || []).includes('SELL PRESSURE') && prediction.direction === 'UP') lev -= maxLev * 0.25;
  if (prediction.direction === 'DOWN') lev *= 0.85;
  return Number(clamp(lev, MIN_FAKE_LEVERAGE, maxLev).toFixed(1));
}

function fakeLiquidationPrice(entry, side, leverage) {
  if (!entry || !leverage) return null;
  const move = LIQUIDATION_MARGIN_RATIO / leverage;
  return side === 'LONG'
    ? Number((entry * Math.max(0.0001, 1 - move)).toFixed(8))
    : Number((entry * (1 + move)).toFixed(8));
}

function openPaperTrade(contestant, prediction, signal, now) {
  if (!signal?.price || contestant.position) return null;
  if (!['UP', 'DOWN'].includes(prediction.direction)) return { action: 'WATCH_ONLY', reason: 'لا توجد صفقة وهمية لأن القرار مراقبة فقط' };
  const equity = computeEquity(contestant);
  const leverage = chooseFakeLeverage(contestant, prediction, signal);
  const baseRisk = 0.06 + Math.max(0, prediction.conviction - 55) / 320 + Math.max(0, contestant.risk) / 420;
  const marginPct = clamp(baseRisk, 0.025, Math.min(0.18, MAX_PAPER_RISK));
  let margin = Math.max(10, equity * marginPct);
  margin = Math.min(margin, contestant.cash * 0.88);
  if (margin < 8) return { action: 'SKIP', reason: 'رصيد وهمي غير كافٍ للهامش' };
  const notional = margin * leverage;
  const fee = notional * PAPER_FEE_BPS / 10000;
  const entrySlippage = notional * PAPER_SLIPPAGE_BPS / 10000;
  if (contestant.cash < margin + fee + entrySlippage) margin = Math.max(0, contestant.cash - fee - entrySlippage);
  const qty = notional / signal.price;
  const side = prediction.direction === 'UP' ? 'LONG' : 'SHORT';
  const liquidationPrice = fakeLiquidationPrice(signal.price, side, leverage);
  const position = {
    id: randomUUID(),
    symbol: signal.symbol,
    side,
    qty,
    notional,
    margin,
    leverage,
    liquidationPrice,
    entryPrice: signal.price,
    openedAt: now,
    entryFee: fee,
    entrySlippage,
    strategy: contestant.style.key,
    tags: [...(prediction.signalTags || [])],
    basis: prediction.basis,
    fixedFormulaId: prediction.fixedFormulaId || null,
    sourceFormulaOriginalLine: prediction.sourceFormulaOriginalLine || null,
    sourceFormulaEquation: prediction.sourceFormulaEquation || null,
    sprintObjective: 'أعلى ربح وهمي خلال 10 دقائق بأسرع وقت مع تحمل خطر التصفية الوهمية'
  };
  contestant.cash = Math.max(0, contestant.cash - margin - fee - entrySlippage);
  contestant.position = position;
  contestant.trades += 1;
  hiveMemory.totalPaperTrades += 1;
  return {
    action: 'OPEN_FAKE_LEVERAGED',
    positionId: position.id,
    side,
    symbol: signal.symbol,
    margin: Number(margin.toFixed(2)),
    notional: Number(notional.toFixed(2)),
    leverage,
    entryPrice: signal.price,
    liquidationPrice,
    fee: Number(fee.toFixed(4)),
    entrySlippage: Number(entrySlippage.toFixed(4)),
    totalEntryCost: Number((fee + entrySlippage).toFixed(4)),
    objective: '10m max-profit sprint',
    text: `${contestant.callsign} فتح ${side} وهمي ${leverage}x على ${signal.symbol}: هامش ${formatUsd(margin)} / حجم ${formatUsd(notional)}`
  };
}

function closePaperTrade(contestant, prediction, exitPrice, now) {
  const pos = contestant.position;
  if (!pos || pos.symbol !== prediction.symbol) return null;
  const sideMult = pos.side === 'LONG' ? 1 : -1;
  const movePct = pos.entryPrice > 0 ? ((exitPrice - pos.entryPrice) / pos.entryPrice) * 100 : 0;
  const grossPnl = (exitPrice - pos.entryPrice) * pos.qty * sideMult;
  const closeNotional = pos.qty * exitPrice;
  const exitFee = closeNotional * PAPER_FEE_BPS / 10000;
  const exitSlippage = closeNotional * PAPER_SLIPPAGE_BPS / 10000;
  const entryFee = Number(pos.entryFee || 0);
  const entrySlippage = Number(pos.entrySlippage || 0);
  const totalFees = entryFee + exitFee;
  const totalSlippage = entrySlippage + exitSlippage;
  const liquidated = grossPnl <= -Number(pos.margin || pos.notional) * LIQUIDATION_MARGIN_RATIO;
  let netPnl;
  let cashBack;
  if (liquidated) {
    netPnl = -Number(pos.margin || 0) - entryFee - entrySlippage;
    cashBack = 0;
    contestant.sprint.liquidations += 1;
  } else {
    netPnl = grossPnl - totalFees - totalSlippage;
    cashBack = Number(pos.margin || pos.notional || 0) + grossPnl - exitFee - exitSlippage;
  }
  contestant.cash += Math.max(0, cashBack);
  contestant.realizedPnl += netPnl;
  const heldMs = now - pos.openedAt;
  const minutes = Math.max(0.05, heldMs / 60000);
  const margin = Math.max(1, Number(pos.margin || pos.notional));
  const grossPnlPctOnMargin = Number(((grossPnl / margin) * 100).toFixed(4));
  const feesPctOnMargin = Number(((totalFees / margin) * 100).toFixed(4));
  const slippagePctOnMargin = Number(((totalSlippage / margin) * 100).toFixed(4));
  const netPnlPctOnMargin = Number(((netPnl / margin) * 100).toFixed(4));
  let result = 'NEUTRAL';
  if (liquidated || netPnlPctOnMargin < -MAX_NEUTRAL_LOSS_PCT) result = 'LOSS';
  else if (netPnlPctOnMargin > MIN_NET_PROFIT_PCT) result = 'WIN';
  const velocity = Number((netPnlPctOnMargin / minutes).toFixed(3));
  contestant.sprint.velocityScore = Number(((contestant.sprint.velocityScore || 0) + (result === 'NEUTRAL' ? 0 : velocity)).toFixed(3));
  contestant.position = null;
  return {
    action: liquidated ? 'FAKE_LIQUIDATED' : 'CLOSE_FAKE_LEVERAGED',
    result,
    isWin: result === 'WIN',
    isLoss: result === 'LOSS',
    isNeutral: result === 'NEUTRAL',
    side: pos.side,
    symbol: pos.symbol,
    entryPrice: pos.entryPrice,
    exitPrice,
    direction: prediction.direction,
    movePct: Number(movePct.toFixed(4)),
    margin: Number((pos.margin || 0).toFixed(2)),
    notional: Number(pos.notional.toFixed(2)),
    leverage: pos.leverage || 1,
    liquidationPrice: pos.liquidationPrice || null,
    grossPnl: Number(grossPnl.toFixed(4)),
    netPnl: Number(netPnl.toFixed(4)),
    grossPnlPct: grossPnlPctOnMargin,
    netPnlPct: netPnlPctOnMargin,
    fees: Number(totalFees.toFixed(4)),
    slippage: Number(totalSlippage.toFixed(4)),
    feesPct: feesPctOnMargin,
    slippagePct: slippagePctOnMargin,
    minProfitPct: MIN_NET_PROFIT_PCT,
    neutralBandPct: MAX_NEUTRAL_LOSS_PCT,
    pnlPct: netPnlPctOnMargin,
    pnlVelocityPerMin: velocity,
    liquidated,
    countedAsWin: result === 'WIN',
    proofEligible: true,
    heldMs
  };
}

function computeEquity(contestant) {
  let equity = Number(contestant.cash || 0);
  const pos = contestant.position;
  if (!pos) return equity;
  const price = currentPrices.get(pos.symbol)?.price || pos.entryPrice;
  const sideMult = pos.side === 'LONG' ? 1 : -1;
  const grossPnl = (price - pos.entryPrice) * pos.qty * sideMult;
  const margin = Number(pos.margin || pos.notional || 0);
  const livePositionValue = Math.max(0, margin + grossPnl);
  return equity + livePositionValue;
}

function analyzeOutcome(contestant, prediction, outcome, signal) {
  const snap = prediction.signalSnapshot || {};
  const tags = snap.tags || prediction.signalTags || [];
  const pnl = outcome.paperResult?.netPnl ?? 0;
  let mood;
  let analysis;
  let nextRule;

  const tradeResult = outcome.tradeResult || outcome.paperResult?.result || 'NO_TRADE';

  if (tradeResult === 'WIN') {
    mood = 'REPEAT_WINNER';
    const pattern = tags.slice(0, 3).join(' + ') || prediction.indicator;
    analysis = `ربح/أصاب لأن ${pattern} سبق الحركة على ${prediction.symbol}. سيكرر نفس المنهج عند تكرار العلامات.`;
    nextRule = `كرر ${contestant.style.ar} عندما تظهر ${pattern} مع conviction أعلى من ${Math.max(45, prediction.conviction - 8)}.`;
    contestant.learning.confidence = clamp(contestant.learning.confidence + 4, 5, 100);
    contestant.minConviction = clamp(contestant.minConviction - 1, 35, 85);
    for (const tag of tags) contestant.learning.winsByTag[tag] = (contestant.learning.winsByTag[tag] || 0) + 1;
    contestant.learning.repeatRule = nextRule;
  } else if (tradeResult === 'LOSS') {
    mood = 'RECALIBRATE_LOSS';
    const cause = classifyLossCause(prediction, snap, outcome.movePct, signal);
    analysis = `خسر/أخطأ لأن ${cause}. سيخفف الثقة ويرفع شرط الدخول الوهمي.`;
    nextRule = `تجنب ${contestant.style.ar} إذا ظهرت ${cause}.`;
    contestant.learning.confidence = clamp(contestant.learning.confidence - 5, 5, 100);
    contestant.minConviction = clamp(contestant.minConviction + 2, 35, 88);
    for (const tag of tags) contestant.learning.lossesByTag[tag] = (contestant.learning.lossesByTag[tag] || 0) + 1;
    contestant.learning.avoidRule = nextRule;
  } else {
    mood = tradeResult === 'WATCH_ONLY' ? 'WATCH_REVIEW' : 'NEUTRAL_REVIEW';
    analysis = tradeResult === 'WATCH_ONLY'
      ? `مراقبة فقط على ${prediction.symbol}: لم تُفتح صفقة وهمية، لذلك لا تدخل في TRADE_PROOF.`
      : `النتيجة محايدة على ${prediction.symbol}: لا يوجد ربح صافي بعد الرسوم والانزلاق؛ لن تُحسب كفوز.`;
    nextRule = 'لا ترفع وزن المعادلة إلا بعد ربح صافي حقيقي في المحاكاة.';
    contestant.learning.confidence = clamp(contestant.learning.confidence - 0.5, 5, 100);
  }

  contestant.learning.mood = mood;
  contestant.learning.lastAnalysis = analysis;
  return {
    id: randomUUID(),
    timestamp: Date.now(),
    contestantId: contestant.id,
    callsign: contestant.callsign,
    symbol: prediction.symbol,
    direction: prediction.direction,
    hit: outcome.hit,
    movePct: outcome.movePct,
    pnl,
    mood,
    analysis,
    nextRule,
    tags: tags.slice(0, 6)
  };
}

function classifyLossCause(prediction, snap, movePct, signal) {
  const tags = new Set(snap.tags || prediction.signalTags || []);
  const buyPct = Number(snap.buyPct || 0) * 100;
  if (prediction.direction === 'UP' && (snap.bigSellUsd || 0) > (snap.bigBuyUsd || 0) * 1.15) return 'ضغط البيع الكبير كان أقوى من الشراء';
  if (prediction.direction === 'DOWN' && buyPct > 62) return 'المشترون امتصوا البيع ولم يسمحوا بالهبوط';
  if (prediction.direction === 'UP' && (snap.priceRangePct || 0) > 1.4) return 'النطاق كان واسعًا؛ الدخول الوهمي جاء بعد حركة متأخرة';
  if (prediction.direction === 'UP' && movePct < 0) return 'لم يحصل follow-through بعد إشارة الشراء';
  if (prediction.direction === 'DOWN' && movePct > 0) return 'البيع تحول إلى فخ وانعكس السعر للأعلى';
  if (tags.has('NO_CLEAR_PATTERN')) return 'العلامات كانت غير واضحة والروبوت بالغ في الثقة';
  return signal?.tags?.includes('SELL PRESSURE') ? 'ظهر SELL PRESSURE بعد القرار' : 'السوق تحرك عكس المنهج خلال مدة التوقع';
}

function recordHiveOutcome(contestant, prediction, outcome, signal) {
  hiveMemory.totalResolved += 1;
  const tags = (prediction.signalSnapshot?.tags || prediction.signalTags || ['NO_TAG']).slice(0, 8);
  const paper = outcome.paperResult;
  const tradeResult = paper?.result || outcome.tradeResult || (prediction.direction === 'WATCH' ? 'WATCH_ONLY' : 'NO_TRADE');
  const countableTrade = Boolean(paper?.proofEligible && ['WIN', 'LOSS', 'NEUTRAL'].includes(tradeResult));
  const pnl = Number(paper?.netPnl ?? 0);
  const tradeKey = `${getPredictionSignalKey(prediction)}|entry:${Number(prediction.entryPrice || 0).toFixed(2)}|exit:${Number(paper?.exitPrice || 0).toFixed(2)}`;
  const isNewUniqueTrade = countableTrade ? rememberCountedTradeKey(tradeKey) : false;
  const duplicateSkipped = countableTrade && !isNewUniqueTrade;
  const win = tradeResult === 'WIN';
  const loss = tradeResult === 'LOSS';
  const neutral = tradeResult === 'NEUTRAL';
  const keys = [
    `${prediction.symbol}|${contestant.style.key}|${prediction.direction}|STYLE`,
    `${contestant.style.key}|${prediction.direction}|STYLE`,
    ...tags.map(tag => `${prediction.symbol}|${contestant.style.key}|${prediction.direction}|${tag}`),
    ...tags.map(tag => `${prediction.symbol}|GLOBAL|${prediction.direction}|${tag}`),
    ...tags.map(tag => `${contestant.style.key}|${prediction.direction}|${tag}`),
    ...tags.map(tag => `GLOBAL|${prediction.direction}|${tag}`)
  ];

  if (isNewUniqueTrade) {
    for (const key of keys) {
      const st = hiveMemory.patternStats.get(key) || { key, wins: 0, losses: 0, neutrals: 0, pnl: 0, games: 0, lastAt: 0 };
      st.games += 1;
      if (win) st.wins += 1;
      else if (loss) st.losses += 1;
      else if (neutral) st.neutrals = Number(st.neutrals || 0) + 1;
      else st.losses += 1;
      st.pnl += pnl;
      st.lastAt = Date.now();
      hiveMemory.patternStats.set(key, st);
    }
  }

  const formulaReport = createOutcomeFormulaReport(contestant, prediction, { ...outcome, duplicateSkipped, proofCounted: isNewUniqueTrade }, signal);
  pushScientistReport(formulaReport);
  if (isNewUniqueTrade) applyCollectiveMathLearning(formulaReport);
  if (isNewUniqueTrade && win) pushSuccessfulFormulaLog(buildSuccessfulFormulaLog(contestant, prediction, outcome, signal, formulaReport, keys));

  if (outcome.selfReview) {
    pushLimited(hiveMemory.reviews, { ...outcome.selfReview, tradeResult, proofCounted: isNewUniqueTrade, duplicateSkipped }, 160);
    const broad = {
      id: randomUUID(),
      timestamp: Date.now(),
      type: win ? 'WIN_RULE' : loss ? 'LOSS_WARNING' : neutral ? 'NEUTRAL_REVIEW' : 'WATCH_ONLY',
      message: duplicateSkipped
        ? `تم تجاهل نسخة مكررة من نفس الإشارة في TRADE_PROOF: ${prediction.symbol}`
        : outcome.selfReview.nextRule,
      source: contestant.callsign,
      symbol: prediction.symbol,
      tradeResult,
      proofCounted: isNewUniqueTrade,
      tags
    };
    pushLimited(hiveMemory.broadcasts, broad, 80);
    broadcast('hive-share', { broadcast: broad, hive: getHiveSnapshot(), timestamp: Date.now() });
  }
}


function featureVectorFromSignal(signal) {
  const whaleUnit = Math.max(1, Number(config.whaleUsd) || defaultConfig.whaleUsd);
  const buyPct = clamp(Number(signal.buyPct || 0), 0, 1);
  const range = Number(signal.priceRangePct || 0);
  const vector = {
    SCORE: clamp(Number(signal.score || 0) / 100, 0, 1.25),
    BUY_IMBALANCE: clamp((buyPct - 0.5) * 2, -1, 1),
    BIG_BUY: clamp(Number(signal.bigBuyUsd || 0) / (whaleUnit * 4), 0, 2.5),
    BIG_SELL: clamp(Number(signal.bigSellUsd || 0) / (whaleUnit * 4), 0, 2.5),
    NET_WHALE: clamp(Number(signal.netWhale || 0) / (whaleUnit * 4), -2.5, 2.5),
    FLOW: clamp(Number(signal.totalFlow || 0) / (whaleUnit * 8), 0, 2.5),
    RANGE_LOCK: clamp((1.05 - range) / 1.05, -1.4, 1),
    PRICE_LIFT: clamp(Number(signal.priceChangePct || 0) / 1.2, -2, 2),
    ABSORPTION: signal.absorption ? 1 : 0,
    WHALE_COUNT: clamp(Number(signal.whaleTradeCount || 0) / 5, 0, 2)
  };
  return vector;
}

function initialBotWeights(contestant) {
  const base = {
    SCORE: 0.30,
    BUY_IMBALANCE: 0.25,
    BIG_BUY: 0.20,
    BIG_SELL: -0.22,
    NET_WHALE: 0.28,
    FLOW: 0.16,
    RANGE_LOCK: 0.14,
    PRICE_LIFT: 0.18,
    ABSORPTION: 0.20,
    WHALE_COUNT: 0.10
  };
  const key = contestant?.style?.key || '';
  if (key === 'SELL_PRESSURE') { base.BIG_SELL = 0.33; base.BUY_IMBALANCE = -0.16; base.NET_WHALE = -0.25; base.PRICE_LIFT = -0.12; }
  if (key === 'ABSORPTION') { base.ABSORPTION = 0.44; base.RANGE_LOCK = 0.34; base.FLOW = 0.20; }
  if (key === 'BIG_BUY') { base.BIG_BUY = 0.48; base.NET_WHALE = 0.24; }
  if (key === 'RANGE_LOCK') { base.RANGE_LOCK = 0.42; base.FLOW = 0.23; }
  if (key === 'PRICE_LIFT' || key === 'MICRO_LIFT') { base.PRICE_LIFT = 0.42; base.BUY_IMBALANCE = 0.22; }
  const variant = Number(contestant?.variant || 0) / 100;
  for (const k of Object.keys(base)) base[k] = Number((base[k] + variant).toFixed(4));
  return base;
}

function buildDecisionEquation(contestant, signal, ctx = {}) {
  const x = featureVectorFromSignal(signal);
  const w0 = initialBotWeights(contestant);
  const learned = contestant.learning?.learnedWeights || {};
  const terms = Object.keys(x).map(k => {
    const globalW = Number(hiveMemory.mathWeights.get(k) || 0);
    const botLearned = Number(learned[k] || 0);
    const w = Number((w0[k] + globalW + botLearned).toFixed(4));
    const value = Number(x[k].toFixed(4));
    return { feature: k, w, value, product: Number((w * value).toFixed(4)) };
  });
  const raw = terms.reduce((sum, t) => sum + t.product, 0);
  const pressure = Number((50 + raw * 38).toFixed(2));
  const equation = `C = 50 + 38Σ(wᵢ·xᵢ) + H; C=${pressure.toFixed(1)} قبل الفلاتر`;
  const latex = `C=50+38\\sum_i w_i x_i + H`;
  const strongest = terms.slice().sort((a, b) => Math.abs(b.product) - Math.abs(a.product)).slice(0, 5);
  return {
    equation,
    sourceFormulaOriginalLine: prediction.sourceFormulaOriginalLine || null,
    sourceFormulaEquation: prediction.sourceFormulaEquation || null,
    fixedFormulaId: prediction.fixedFormulaId || null,
    latex,
    raw: Number(raw.toFixed(4)),
    pressure,
    terms: strongest,
    vector: x,
    baseStyle: contestant.style?.key,
    hiveBoost: ctx.hiveContext?.boost || 0,
    mathBoost: ctx.mathContext?.boost || 0,
    finalConviction: null,
    finalDirection: ctx.direction || 'WATCH'
  };
}

function createDecisionFormulaReport(contestant, prediction, signal, reading, now = Date.now()) {
  const formula = reading.formula || buildDecisionEquation(contestant, signal, reading);
  return {
    id: randomUUID(),
    kind: 'DECISION_FORMULA',
    timestamp: now,
    phase: 'قبل النتيجة',
    contestantId: contestant.id,
    callsign: contestant.callsign,
    style: contestant.style.name,
    styleKey: contestant.style.key,
    styleAr: contestant.style.ar,
    symbol: prediction.symbol,
    direction: prediction.direction,
    conviction: prediction.conviction,
    equation: formula.equation,
    sourceFormulaOriginalLine: prediction.sourceFormulaOriginalLine || formula.fixedFormulaOriginalLine || null,
    sourceFormulaEquation: prediction.sourceFormulaEquation || formula.fixedFormulaEquation || null,
    fixedFormulaId: prediction.fixedFormulaId || formula.fixedFormulaId || null,
    latex: formula.latex,
    terms: formula.terms,
    vector: formula.vector,
    result: 'OPEN',
    pnl: 0,
    message: `${contestant.callsign} نشر معادلة قرار على ${prediction.symbol}: ${formula.equation}`,
    teaching: `أراقب ${prediction.symbol} بهدف سباق 10 دقائق؛ الرافعة الوهمية ${prediction.paperTrade?.leverage || 1}x، وأعلى حدود المعادلة: ${formula.terms.map(t => `${t.feature}=${t.product}`).join(', ') || 'لا توجد حدود قوية'}.`
  };
}

function strictSign(n) {
  const x = Number(n);
  if (!Number.isFinite(x) || Object.is(x, -0) || x === 0) return 0;
  return x > 0 ? 1 : -1;
}

function createOutcomeFormulaReport(contestant, prediction, outcome, signal) {
  const snap = prediction.signalSnapshot || {};
  const vector = snap.vector || featureVectorFromSignal({ ...snap, tags: prediction.signalTags || [] });
  const paper = outcome.paperResult || null;
  const tradeResult = paper?.result || outcome.tradeResult || (prediction.direction === 'WATCH' ? 'WATCH_ONLY' : 'NO_TRADE');
  const netPnl = Number(paper?.netPnl ?? 0);
  const netPnlPct = Number(paper?.netPnlPct ?? 0);
  const grossPnlPct = Number(paper?.grossPnlPct ?? 0);
  const feesPct = Number(paper?.feesPct ?? 0);
  const slippagePct = Number(paper?.slippagePct ?? 0);
  const movePct = Number(outcome.movePct || 0);
  const proofCounted = Boolean(outcome.proofCounted);
  const duplicateSkipped = Boolean(outcome.duplicateSkipped);
  let reward = 0;
  if (tradeResult === 'WIN') reward = Math.abs(movePct) + Math.abs(netPnlPct);
  else if (tradeResult === 'LOSS') reward = -(Math.abs(movePct) + Math.abs(netPnlPct));
  else reward = 0;
  // Mathematically strict: neutral/watch/no-trade outcomes have R=0 even if gross move was positive.
  if (!['WIN', 'LOSS'].includes(tradeResult)) reward = 0;
  const terms = Object.entries(vector).map(([feature, value]) => ({
    feature,
    value: Number(Number(value).toFixed(4)),
    gradient: Number((reward * Number(value || 0)).toFixed(4))
  })).sort((a, b) => Math.abs(b.gradient) - Math.abs(a.gradient)).slice(0, 7);
  const equation = `gross=${grossPnlPct.toFixed(4)}% - fees=${feesPct.toFixed(4)}% - slippage=${slippagePct.toFixed(4)}% => net=${netPnlPct.toFixed(4)}%; result=${tradeResult}; R=${reward.toFixed(4)}; Δwᵢ=η·R·xᵢ`;
  const latex = `R=\operatorname{sign}(net\,PnL)\left(|move|+|net\,PnL|\right),\quad \Delta w_i=\eta R x_i`;
  const kind = tradeResult === 'WIN' ? 'WIN_PROOF' : tradeResult === 'LOSS' ? 'LOSS_PROOF' : tradeResult === 'NEUTRAL' ? 'NEUTRAL_PROOF' : 'WATCH_SCORE_ONLY';
  return {
    id: randomUUID(),
    kind,
    timestamp: Date.now(),
    phase: tradeResult === 'WIN' ? 'برهان ربح صافي' : tradeResult === 'LOSS' ? 'تشريح خسارة صافية' : tradeResult === 'NEUTRAL' ? 'محايد بعد الرسوم والانزلاق' : 'مراقبة فقط',
    contestantId: contestant.id,
    callsign: contestant.callsign,
    style: contestant.style.name,
    styleAr: contestant.style.ar,
    symbol: prediction.symbol,
    direction: prediction.direction,
    conviction: prediction.conviction,
    equation,
    sourceFormulaOriginalLine: prediction.sourceFormulaOriginalLine || null,
    sourceFormulaEquation: prediction.sourceFormulaEquation || null,
    fixedFormulaId: prediction.fixedFormulaId || null,
    latex,
    terms,
    vector,
    reward: Number(reward.toFixed(4)),
    result: tradeResult,
    hit: tradeResult === 'WIN',
    pnl: netPnl,
    netPnl,
    netPnlPct,
    grossPnlPct,
    feesPct,
    slippagePct,
    movePct,
    notional: Number(paper?.notional || prediction.paperTrade?.notional || 0),
    proofCounted,
    duplicateSkipped,
    message: `${contestant.callsign} أرسل نتيجة رياضية: ${equation}`,
    teaching: duplicateSkipped
      ? 'هذه نسخة مكررة من نفس الإشارة؛ لا تدخل في TRADE_PROOF ولا تعدل أوزان الخلية.'
      : tradeResult === 'WIN'
        ? `الخلية سترفع وزن الحدود فقط لأن الربح الصافي تجاوز ${MIN_NET_PROFIT_PCT}%.`
        : tradeResult === 'LOSS'
          ? 'الخلية ستخفض وزن الحدود لأن الصفقة الوهمية خسرت صافيًا.'
          : tradeResult === 'NEUTRAL'
            ? 'لا تعلم إيجابي ولا proof win: الحركة لم تغط الرسوم والانزلاق.'
            : 'WATCH_SCORE فقط، لا يوجد دخول وهمي ولا proof.'
  };
}


function buildSuccessfulFormulaLog(contestant, prediction, outcome, signal, formulaReport, keys = []) {
  const now = Date.now();
  const pnl = Number(outcome.paperResult?.netPnl ?? 0);
  const movePct = Number(outcome.movePct || 0);
  const tags = (prediction.signalSnapshot?.tags || prediction.signalTags || []).slice(0, 8);
  const relatedPatterns = keys.map(key => {
    const st = hiveMemory.patternStats.get(key);
    if (!st) return null;
    return {
      key,
      wins: st.wins,
      losses: st.losses,
      games: st.games,
      winRate: Number((st.wins / Math.max(1, st.games) * 100).toFixed(2)),
      pnl: Number(st.pnl.toFixed(2)),
      perfectSoFar: st.games >= MIN_PROOF_SAMPLES && st.losses === 0 && st.wins > 0
    };
  }).filter(Boolean).sort((a, b) => (b.perfectSoFar - a.perfectSoFar) || b.games - a.games || b.winRate - a.winRate).slice(0, 8);
  const entry = {
    id: randomUUID(),
    timestamp: now,
    isoTime: new Date(now).toISOString(),
    symbol: prediction.symbol,
    direction: prediction.direction,
    bot: contestant.callsign,
    botId: contestant.id,
    style: contestant.style.name,
    styleKey: contestant.style.key,
    result: outcome.paperResult?.result || 'WIN',
    hit: outcome.hit,
    pnl: Number(pnl.toFixed(4)),
    netPnlPct: Number((outcome.paperResult?.netPnlPct || 0).toFixed(4)),
    grossPnlPct: Number((outcome.paperResult?.grossPnlPct || 0).toFixed(4)),
    feesPct: Number((outcome.paperResult?.feesPct || 0).toFixed(4)),
    slippagePct: Number((outcome.paperResult?.slippagePct || 0).toFixed(4)),
    movePct: Number(movePct.toFixed(4)),
    entryPrice: prediction.entryPrice,
    exitPrice: outcome.paperResult?.exitPrice ?? null,
    leverage: prediction.paperTrade?.leverage || 1,
    notional: Number(outcome.paperResult?.notional || prediction.paperTrade?.notional || 0),
    conviction: Number(prediction.conviction || 0),
    equation: formulaReport?.equation || prediction.formula?.equation || 'R = sign(PnL)·(|move|+|PnL|); Δwᵢ = η·R·xᵢ',
    sourceFormulaOriginalLine: prediction.sourceFormulaOriginalLine || formulaReport?.sourceFormulaOriginalLine || null,
    sourceFormulaEquation: prediction.sourceFormulaEquation || formulaReport?.sourceFormulaEquation || null,
    fixedFormulaId: prediction.fixedFormulaId || formulaReport?.fixedFormulaId || null,
    decisionEquation: prediction.formula?.equation || null,
    latex: formulaReport?.latex || null,
    reward: formulaReport?.reward ?? null,
    proofCounted: true,
    minimumNetProfitPct: MIN_NET_PROFIT_PCT,
    terms: (formulaReport?.terms || []).slice(0, 10),
    tags,
    relatedPatterns,
    copyLine: '',
    copyBlock: ''
  };
  entry.copyLine = formatSuccessLogLine(entry);
  entry.copyBlock = formatSuccessLogBlock(entry);
  return entry;
}

function pushSuccessfulFormulaLog(entry) {
  if (!entry) return;
  pushLimited(hiveMemory.successFormulaLogs, entry, SUCCESS_FORMULA_LOG_MAX);
  broadcast('success-log', { log: entry, count: hiveMemory.successFormulaLogs.length, timestamp: Date.now() });
}

function formatSuccessLogLine(x) {
  const t = x.isoTime || new Date(x.timestamp || Date.now()).toISOString();
  const best = x.relatedPatterns?.[0];
  const proof = best ? `pattern=${best.wins}/${best.games}${best.losses === 0 ? ' PERFECT_SO_FAR' : ''}` : 'pattern=n/a';
  return `[${t}] ${x.symbol} ${x.direction} | ${x.bot} ${x.styleKey} | WIN net=${Number(x.netPnlPct||0).toFixed(4)}% gross=${Number(x.grossPnlPct||0).toFixed(4)}% fees+slip=${(Number(x.feesPct||0)+Number(x.slippagePct||0)).toFixed(4)}% move=${Number(x.movePct||0).toFixed(4)}% lev=${x.leverage}x | ${proof} | locked-source: ${x.sourceFormulaOriginalLine || 'n/a'} | equation: ${x.equation}`;
}

function formatSuccessLogBlock(x) {
  const best = x.relatedPatterns?.[0];
  const terms = (x.terms || []).map(t => `  - ${t.feature}: value=${t.value ?? ''} gradient=${t.gradient ?? ''}`).join('\n');
  const patterns = (x.relatedPatterns || []).map(p => `  - ${p.key}: wins=${p.wins}, losses=${p.losses}, games=${p.games}, winRate=${p.winRate}%, perfect=${p.perfectSoFar}`).join('\n');
  return [
    `SUCCESS EQUATION LOG`,
    `Time: ${x.isoTime || new Date(x.timestamp || Date.now()).toISOString()}`,
    `Symbol: ${x.symbol}`,
    `Direction: ${x.direction}`,
    `Bot: ${x.bot} / ${x.style}`,
    `Result: WIN | NetPnL=${Number(x.netPnlPct||0).toFixed(4)}% | Gross=${Number(x.grossPnlPct||0).toFixed(4)}% | Fees+Slippage=${(Number(x.feesPct||0)+Number(x.slippagePct||0)).toFixed(4)}% | Move=${Number(x.movePct||0).toFixed(4)}% | Leverage=${x.leverage}x`,
    `Equation: ${x.equation}`,
    x.decisionEquation ? `Decision equation: ${x.decisionEquation}` : null,
    best ? `Best related proof: ${best.key} => ${best.wins}/${best.games}, losses=${best.losses}, winRate=${best.winRate}%` : null,
    `Terms:\n${terms || '  - none'}`,
    `Related patterns:\n${patterns || '  - none'}`,
    `Tags: ${(x.tags || []).join(', ') || 'none'}`,
    `Note: paper-simulation only; not financial advice; no real trade was placed.`
  ].filter(Boolean).join('\n');
}

function pushScientistReport(report) {
  if (!report) return;
  hiveMemory.totalFormulaReports += 1;
  pushLimited(hiveMemory.formulaReports, report, HIVE_REPORT_MAX);
  pushLimited(hiveMemory.scientistDebates, {
    id: report.id,
    timestamp: report.timestamp,
    speaker: report.callsign,
    symbol: report.symbol,
    result: report.result,
    equation: report.equation,
    teaching: report.teaching
  }, 160);
  broadcast('math-report', { report, hive: getHiveSnapshot(), timestamp: Date.now() });
}

function applyCollectiveMathLearning(report) {
  if (!report || !report.terms?.length || !Number.isFinite(Number(report.reward))) return;
  const reward = Number(report.reward);
  if (!reward) return;
  const lr = HIVE_LEARNING_RATE;
  const changed = [];
  for (const term of report.terms) {
    const feature = term.feature;
    const delta = clamp(lr * reward * Number(term.value || 0), -0.07, 0.07);
    const oldWeight = Number(hiveMemory.mathWeights.get(feature) || 0);
    const next = clamp(oldWeight + delta, -0.55, 0.55);
    hiveMemory.mathWeights.set(feature, Number(next.toFixed(5)));
    const st = hiveMemory.mathStats.get(feature) || { feature, updates: 0, up: 0, down: 0, netReward: 0, lastDelta: 0 };
    st.updates += 1;
    if (delta >= 0) st.up += 1; else st.down += 1;
    st.netReward += reward;
    st.lastDelta = Number(delta.toFixed(5));
    hiveMemory.mathStats.set(feature, st);
    changed.push({ feature, delta: Number(delta.toFixed(5)), weight: Number(next.toFixed(5)) });
  }
  hiveMemory.collectiveVersion += 1;
  hiveMemory.consensus = buildConsensusFromWeights(changed, report);
  teachAllBotsFromReport(report, changed);
  broadcast('hive-learn', { consensus: hiveMemory.consensus, weights: getTopMathWeights(12), report, timestamp: Date.now() });
}

function teachAllBotsFromReport(report, changed) {
  const winner = report.result === 'WIN';
  for (const bot of contestants) {
    bot.learning.learnedFrom += 1;
    for (const change of changed) {
      const affinity = bot.style?.key === report.styleKey ? 1.15 : bot.style?.direction === report.direction ? 0.7 : 0.36;
      const old = Number(bot.learning.learnedWeights[change.feature] || 0);
      bot.learning.learnedWeights[change.feature] = Number(clamp(old + change.delta * affinity * 0.18, -0.22, 0.22).toFixed(5));
    }
    if (winner) bot.learning.confidence = clamp(bot.learning.confidence + 0.05, 5, 100);
    else bot.learning.confidence = clamp(bot.learning.confidence - 0.04, 5, 100);
  }
}

function buildConsensusFromWeights(changed, report) {
  const top = getTopMathWeights(6);
  const strongest = top.map(x => `${x.feature}:${x.weight > 0 ? '+' : ''}${x.weight}`).join(' | ') || 'no weights yet';
  return {
    timestamp: Date.now(),
    version: hiveMemory.collectiveVersion,
    source: report.callsign,
    symbol: report.symbol,
    result: report.result,
    equation: `Cₕive = 50 + 38Σ((w_bot+w_hive)·x)` ,
    teaching: `آخر تحديث جماعي من ${report.callsign}. أقوى أوزان الخلية الآن: ${strongest}.`,
    changed
  };
}

function getTopMathWeights(limit = 12) {
  return [...hiveMemory.mathWeights.entries()]
    .map(([feature, weight]) => ({ feature, weight: Number(weight.toFixed(5)), strength: Math.abs(weight), stats: hiveMemory.mathStats.get(feature) || null }))
    .sort((a, b) => b.strength - a.strength)
    .slice(0, limit);
}

function getMathConsensus() {
  return hiveMemory.consensus || {
    timestamp: hiveMemory.startedAt,
    version: hiveMemory.collectiveVersion,
    equation: 'Cₕive = 50 + 38Σ((w_bot+w_hive)·x)',
    teaching: 'الخلية تنتظر أول برهان ربح أو تشريح خسارة حتى تبدأ تعديل الأوزان رياضيًا.',
    changed: []
  };
}

function getCollectiveMathBoost(contestant, signal, direction) {
  if (!direction || direction === 'WATCH') return { boost: 0, note: '' };
  const vector = featureVectorFromSignal(signal);
  let raw = 0;
  let best = null;
  for (const [feature, value] of Object.entries(vector)) {
    const w = Number(hiveMemory.mathWeights.get(feature) || 0) + Number(contestant.learning?.learnedWeights?.[feature] || 0);
    const product = w * Number(value || 0);
    raw += product;
    if (!best || Math.abs(product) > Math.abs(best.product)) best = { feature, product, w, value };
  }
  if (direction === 'DOWN') raw *= -0.85;
  const boost = clamp(raw * 24, -16, 16);
  return {
    boost,
    note: best && Math.abs(best.product) > 0.005 ? `${best.feature} ${best.product >= 0 ? '+' : ''}${best.product.toFixed(2)}` : ''
  };
}

function getSharedHiveBoost(contestant, signal, direction) {
  if (!direction || direction === 'WATCH') return { boost: 0, note: '' };
  const tags = featureLabelsFromSignal(signal).slice(0, 6);
  let boost = 0;
  let best = null;
  for (const tag of tags) {
    for (const key of [`${contestant.style.key}|${direction}|${tag}`, `GLOBAL|${direction}|${tag}`]) {
      const st = hiveMemory.patternStats.get(key);
      if (!st || st.games < 3) continue;
      const acc = st.wins / Math.max(1, st.games);
      const edge = (acc - 0.5) * 18 + Math.max(-6, Math.min(6, st.pnl / 30));
      boost += edge;
      if (!best || edge > best.edge) best = { tag, acc, edge, games: st.games };
    }
  }
  boost = clamp(boost, -18, 18);
  return {
    boost,
    note: best ? `${best.tag} ${Math.round(best.acc * 100)}%/${best.games}` : ''
  };
}


function getProofSnapshot() {
  const now = Date.now();
  const rows = [...hiveMemory.patternStats.values()]
    .filter(st => st.games >= Math.max(1, Math.floor(MIN_PROOF_SAMPLES / 2)))
    .map(st => {
      const parts = String(st.key || '').split('|');
      const symbolLike = parts[0] && /USDT$/.test(parts[0]);
      const symbol = symbolLike ? parts[0] : 'GLOBAL';
      const strategy = symbolLike ? (parts[1] || 'GLOBAL') : (parts[0] || 'GLOBAL');
      const direction = symbolLike ? (parts[2] || 'WATCH') : (parts[1] || 'WATCH');
      const tag = symbolLike ? parts.slice(3).join('|') : parts.slice(2).join('|');
      const closedTrials = Number(st.games || 0);
      const neutrals = Number(st.neutrals || 0);
      const winRate = closedTrials ? st.wins / closedTrials : 0;
      const avgPnl = closedTrials ? st.pnl / closedTrials : 0;
      const samplePower = Math.min(1, closedTrials / Math.max(1, MIN_PROOF_SAMPLES));
      const perfect = closedTrials >= MIN_PROOF_SAMPLES && st.wins === closedTrials && st.losses === 0 && neutrals === 0;
      const status = perfect ? 'PERFECT_SO_FAR' : (closedTrials >= MIN_PROOF_SAMPLES && winRate >= 0.8 ? 'HIGH_PROBABILITY' : 'UNDER_STUDY');
      const proofScore = perfect
        ? 100
        : clamp(winRate * 82 + samplePower * 18 + Math.max(-10, Math.min(10, avgPnl / 2)), 0, 99.9);
      return {
        key: st.key,
        symbol,
        strategy,
        direction,
        tag: tag || 'STYLE',
        wins: st.wins,
        losses: st.losses,
        neutrals,
        games: closedTrials,
        realClosedTrials: closedTrials,
        pnl: Number(st.pnl.toFixed(2)),
        avgPnl: Number(avgPnl.toFixed(3)),
        winRate: Number((winRate * 100).toFixed(2)),
        proofScore: Number(proofScore.toFixed(2)),
        perfect,
        status,
        lastAt: st.lastAt,
        ageSec: st.lastAt ? Math.round((now - st.lastAt) / 1000) : null,
        equation: `${direction}_TRADE_PROOF = real_wins(${st.wins}) / real_closed_trials(${closedTrials}); neutrals=${neutrals}; losses=${st.losses}${perfect ? ' = 100% PERFECT-SO-FAR' : ''}`,
        alert: perfect ? '👑 PERFECT-SO-FAR WATCH' : (status === 'HIGH_PROBABILITY' ? '🧠 HIGH PROBABILITY' : '🧪 UNDER STUDY')
      };
    });

  const perfect = rows.filter(r => r.perfect)
    .sort((a, b) => b.games - a.games || b.avgPnl - a.avgPnl)
    .slice(0, 18);
  const highProbability = rows.filter(r => !r.perfect && r.status === 'HIGH_PROBABILITY')
    .sort((a, b) => b.proofScore - a.proofScore || b.games - a.games)
    .slice(0, 18);
  const underStudy = rows.filter(r => r.status === 'UNDER_STUDY')
    .sort((a, b) => b.games - a.games || b.proofScore - a.proofScore)
    .slice(0, 18);

  const coinMap = new Map();
  for (const r of rows.filter(r => r.symbol !== 'GLOBAL')) {
    const c = coinMap.get(r.symbol) || { symbol: r.symbol, patterns: 0, perfect: 0, highProbability: 0, games: 0, wins: 0, losses: 0, bestProof: 0, bestPattern: null };
    c.patterns += 1;
    c.games += r.games;
    c.wins += r.wins;
    c.losses += r.losses;
    if (r.perfect) c.perfect += 1;
    if (r.status === 'HIGH_PROBABILITY') c.highProbability += 1;
    if (r.proofScore > c.bestProof) { c.bestProof = r.proofScore; c.bestPattern = r; }
    coinMap.set(r.symbol, c);
  }
  const coinBrains = [...coinMap.values()].map(c => {
    const sampleScore = Math.min(40, c.games / 4);
    const quality = Math.min(40, c.bestProof * 0.4);
    const perfectBonus = Math.min(20, c.perfect * 5 + c.highProbability * 2);
    return { ...c, understanding: Number(clamp(sampleScore + quality + perfectBonus, 0, 100).toFixed(1)) };
  }).sort((a, b) => b.understanding - a.understanding || b.games - a.games).slice(0, 100);

  return {
    timestamp: now,
    minProofSamples: MIN_PROOF_SAMPLES,
    perfectAlertOnly: PERFECT_ALERT_ONLY,
    definition: 'PERFECT-SO-FAR means the pattern has zero losses inside the live paper-simulation record and reached the minimum sample count. It is not a guarantee of future profit.',
    perfect,
    highProbability,
    underStudy,
    coinBrains,
    counts: { perfect: perfect.length, highProbability: highProbability.length, trackedCoins: coinBrains.length, totalPatterns: rows.length }
  };
}

function getHiveSnapshot() {
  const equities = contestants.map(c => computeEquity(c));
  const totalEquity = equities.reduce((a, b) => a + b, 0);
  const totalCash = contestants.reduce((sum, c) => sum + Number(c.cash || 0), 0);
  const realizedPnl = contestants.reduce((sum, c) => sum + Number(c.realizedPnl || 0), 0);
  const openPositions = contestants.filter(c => c.position).length;
  const sorted = contestants.slice().sort((a, b) => computeEquity(b) - computeEquity(a));
  const bestBot = sorted[0];
  const worstBot = sorted.at(-1);

  const topPatterns = [...hiveMemory.patternStats.values()]
    .filter(st => st.games >= 2)
    .map(st => ({
      ...st,
      winRate: st.games ? st.wins / st.games : 0,
      edge: (st.games ? st.wins / st.games : 0) * 100 + Math.max(-20, Math.min(20, st.pnl / 10))
    }))
    .sort((a, b) => b.edge - a.edge || b.games - a.games)
    .slice(0, 12);

  const dangerPatterns = [...hiveMemory.patternStats.values()]
    .filter(st => st.games >= 2)
    .map(st => ({
      ...st,
      winRate: st.games ? st.wins / st.games : 0,
      edge: (st.games ? st.wins / st.games : 0) * 100 + Math.max(-20, Math.min(20, st.pnl / 10))
    }))
    .sort((a, b) => a.edge - b.edge || b.games - a.games)
    .slice(0, 8);

  const symbolBrains = [...hiveMemory.symbolStats.values()]
    .map(s => ({
      symbol: s.symbol,
      upMoves: s.upMoves,
      downMoves: s.downMoves,
      netMoveLearned: Number(s.netMoveLearned.toFixed(3)),
      topFactors: Object.entries(s.factors || {})
        .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
        .slice(0, 5)
        .map(([tag, score]) => ({ tag, score }))
    }))
    .sort((a, b) => (b.upMoves + b.downMoves) - (a.upMoves + a.downMoves))
    .slice(0, 12);

  const sharedRules = hiveMemory.broadcasts.slice(-12).reverse();
  return {
    timestamp: Date.now(),
    fakeBank: {
      initialPerBot: INITIAL_FAKE_USD,
      totalStarting: INITIAL_FAKE_USD * contestants.length,
      totalEquity: Number(totalEquity.toFixed(2)),
      totalCash: Number(totalCash.toFixed(2)),
      realizedPnl: Number(realizedPnl.toFixed(2)),
      unrealizedPnl: Number((totalEquity - totalCash - openPositions * 0).toFixed(2)),
      totalPnl: Number((totalEquity - INITIAL_FAKE_USD * contestants.length).toFixed(2)),
      openPositions,
      paperTrades: hiveMemory.totalPaperTrades,
      maxFakeLeverage: Number(config.maxFakeLeverage || MAX_FAKE_LEVERAGE),
      twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
      fixedFormulas: fixedFormulaStatus(),
      sprintHorizonSec: Number(config.arenaHorizonSec || 600),
      objective: 'MAX_PROFIT_FASTEST_10M_FAKE_LEVERAGE',
      totalLiquidations: contestants.reduce((sum, c) => sum + Number(c.sprint?.liquidations || 0), 0),
      bestBot: bestBot ? { id: bestBot.id, callsign: bestBot.callsign, equity: Number(computeEquity(bestBot).toFixed(2)), pnl: Number((computeEquity(bestBot) - INITIAL_FAKE_USD).toFixed(2)), velocity: bestBot.sprint?.velocityScore || 0, liquidations: bestBot.sprint?.liquidations || 0, mood: bestBot.learning?.mood } : null,
      worstBot: worstBot ? { id: worstBot.id, callsign: worstBot.callsign, equity: Number(computeEquity(worstBot).toFixed(2)), pnl: Number((computeEquity(worstBot) - INITIAL_FAKE_USD).toFixed(2)), velocity: worstBot.sprint?.velocityScore || 0, liquidations: worstBot.sprint?.liquidations || 0, mood: worstBot.learning?.mood } : null
    },
    topPatterns,
    dangerPatterns,
    symbolBrains,
    lessons: hiveMemory.lessons.slice(-14).reverse(),
    reviews: hiveMemory.reviews.slice(-14).reverse(),
    sharedRules,
    formulaReports: hiveMemory.formulaReports.slice(-22).reverse(),
    liveThoughts: hiveMemory.liveThoughts.slice(-22).reverse(),
    scientistDebates: hiveMemory.scientistDebates.slice(-20).reverse(),
    proof: getProofSnapshot(),
    mathConsensus: getMathConsensus(),
    topWeights: getTopMathWeights(14),
    totalFormulaReports: hiveMemory.totalFormulaReports,
    successfulFormulaLogCount: hiveMemory.successFormulaLogs.length,
    successfulFormulaLogs: hiveMemory.successFormulaLogs.slice(-18).reverse(),
    totalLiveThoughts: hiveMemory.totalLiveThoughts,
    livingPulseMs: SCIENTIST_THINK_MS,
    collectiveVersion: hiveMemory.collectiveVersion,
    twoFormulaOnlyMode: TWO_FORMULA_ONLY_MODE,
    fixedFormulas: fixedFormulaStatus(),
    totalResolved: hiveMemory.totalResolved
  };
}

function pushLimited(arr, item, max) {
  arr.push(item);
  if (arr.length > max) arr.splice(0, arr.length - max);
}

async function initValidSymbols() {
  // V6 uses Binance Futures WebSocket aggTrade streams for live market data.
  // REST exchangeInfo is optional only. If Binance blocks REST from a cloud IP,
  // the game still runs and validates symbols with a safe USDT suffix pattern.
  try {
    log('api', 'V17 Two Formula Only mode: optional exchangeInfo validation starting. REST is not used for scans.');
    const data = await binanceJson('/fapi/v1/exchangeInfo');
    const symbols = Array.isArray(data.symbols) ? data.symbols : [];
    validSymbols = new Set(
      symbols
        .filter(s => s.status === 'TRADING' && s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT')
        .map(s => s.symbol)
    );
    exchangeInfoLoadedAt = Date.now();

    const normalized = await normalizeConfig(config);
    config = normalized.config;

    log('api', `Validated ${validSymbols.size} Binance USDT perpetual symbols. Active loadout: ${describeLoadout(config)}.`);
    if (normalized.rejected.length) {
      log('loadout', `Rejected invalid/default symbols: ${normalized.rejected.join(', ')}`);
    }
    ensureTradeStreams(config.symbols);
    // Bitcoin-only mode: no all-symbol subscription.
    broadcast('config', { config, rejected: normalized.rejected, warnings: normalized.warnings, timestamp: Date.now() });
  } catch (error) {
    validSymbols = new Set();
    exchangeInfoLoadedAt = null;
    log('api-error', `Optional exchangeInfo validation skipped: ${error.message || error}. V17 will continue with BTCUSDT WebSocket data.`);
    const normalized = await normalizeConfig(config);
    config = normalized.config;
    ensureTradeStreams(config.symbols);
    broadcast('config', { config, rejected: normalized.rejected, warnings: normalized.warnings, timestamp: Date.now() });
  }
}

async function normalizeConfig(raw) {
  const warnings = ['Bitcoin-only mode enabled: all symbols are locked to BTCUSDT. All 500 bots, proofs, equations, logs, alerts, and paper trades study Bitcoin only.'];
  const rejected = [];

  const rawSymbols = Array.isArray(raw.symbols)
    ? raw.symbols
    : String(raw.symbols || '').split(/[\s,]+/);
  for (const sym of rawSymbols) {
    const clean = String(sym || '').trim().toUpperCase();
    if (clean && clean !== BTC_ONLY_SYMBOL) rejected.push(clean);
  }

  const whaleUsd = clampNumber(raw.whaleUsd, 100, 100000000, defaultConfig.whaleUsd);
  const minScore = clampNumber(raw.minScore, 1, 100, defaultConfig.minScore);
  const windowSec = clampNumber(raw.windowSec, 10, 600, defaultConfig.windowSec);
  const intervalSec = clampNumber(raw.intervalSec, 3, 300, defaultConfig.intervalSec);
  const arenaHorizonSec = clampNumber(raw.arenaHorizonSec, 60, 14400, defaultConfig.arenaHorizonSec);
  const maxFakeLeverage = clampNumber(raw.maxFakeLeverage, 1, 50, defaultConfig.maxFakeLeverage || MAX_FAKE_LEVERAGE);

  return {
    config: {
      symbols: [BTC_ONLY_SYMBOL],
      whaleUsd,
      minScore,
      windowSec,
      intervalSec,
      arenaHorizonSec,
      maxFakeLeverage
    },
    rejected: [...new Set(rejected)],
    warnings
  };
}

function isAllSymbolToken(_token) {
  return false;
}

function isAllSymbolsRequest(_symbols) {
  return false;
}

function describeLoadout(_cfg = config) {
  return 'Bitcoin-only mode: BTCUSDT';
}

function getScanSymbols(_cfg = config) {
  return [BTC_ONLY_SYMBOL];
}

function ensureTradeStreams(_symbols) {
  const clean = [BTC_ONLY_SYMBOL];
  const key = clean.join(',');
  if (wsState.key === key && wsState.mode === 'selected' && (wsState.connected || wsState.ws)) return;

  closeTradeStream();
  wsState.key = key;
  wsState.mode = 'selected';
  wsState.subscribedSymbols = new Set(clean);
  wsState.discoveredSymbols = new Set(clean);
  wsState.pendingSubscribe = [];
  if (!tradeBuffers.has(BTC_ONLY_SYMBOL)) tradeBuffers.set(BTC_ONLY_SYMBOL, []);

  const streams = clean.map(symbol => `${symbol.toLowerCase()}@aggTrade`).join('/');
  const url = `${BINANCE_WS_STREAM_BASE}${streams}`;
  wsState.url = url;
  openBinanceSocket(url, clean, 'selected');
}

function ensureAllBinanceTradeStreams() {
  ensureTradeStreams([BTC_ONLY_SYMBOL]);
}

function openBinanceSocket(url, labelSymbols, mode) {
  const ws = new WebSocket(url, {
    handshakeTimeout: REQUEST_TIMEOUT_MS,
    headers: { 'User-Agent': 'WhaleHunterRadar/15.0 bitcoin-quant-proof-monitoring-only' }
  });
  wsState.ws = ws;
  wsState.connected = false;

  ws.on('open', () => {
    wsState.connected = true;
    wsState.lastMessageAt = Date.now();
    if (mode === 'all') {
      log('stream', `Bitcoin-only mode online. Subscribed to BTCUSDT Futures aggTrade stream.`);
      if (validSymbols.size) subscribeAllKnownSymbols('exchangeInfo-ready');
    } else {
      log('stream', `Binance Futures WebSocket connected for ${labelSymbols.length} selected symbol(s).`);
    }
    broadcast('stream', {
      connected: true,
      mode,
      symbols: mode === 'all' ? [ALL_SYMBOLS_SENTINEL] : labelSymbols,
      activeSymbolCount: getScanSymbols(config).length,
      subscribedSymbols: wsState.subscribedSymbols.size,
      discoveredSymbols: wsState.discoveredSymbols.size,
      timestamp: Date.now()
    });
  });

  ws.on('message', data => {
    try {
      const packet = JSON.parse(String(data));
      const event = packet.data || packet;
      if (Array.isArray(event)) return ingestMiniTickerArray(event);
      if (event?.e === '24hrMiniTicker') return ingestMiniTickerArray([event]);
      if (event?.e === 'aggTrade') return ingestAggTrade(event);
      if (event?.result === null) return;
      if (event?.code || event?.msg) log('api-error', `WebSocket control message: ${JSON.stringify(event).slice(0, 240)}`);
    } catch (error) {
      log('api-error', `WebSocket message parse failed: ${error.message || error}`);
    }
  });

  ws.on('close', (code, reason) => {
    const wasCurrent = wsState.ws === ws;
    if (!wasCurrent) return;
    wsState.ws = null;
    wsState.connected = false;
    log('stream', `Binance WebSocket closed (${code}). Reconnecting soon. ${reason ? String(reason) : ''}`.trim());
    broadcast('stream', { connected: false, mode: wsState.mode, code, timestamp: Date.now() });
    scheduleStreamReconnect(mode === 'all' ? [ALL_SYMBOLS_SENTINEL] : labelSymbols);
  });

  ws.on('error', error => {
    log('api-error', `Binance WebSocket error: ${error.message || error}`);
    broadcast('stream-error', { message: error.message || String(error), mode: wsState.mode, timestamp: Date.now() });
  });
}

function closeTradeStream() {
  if (wsState.reconnectTimer) clearTimeout(wsState.reconnectTimer);
  wsState.reconnectTimer = null;
  if (wsState.ws) {
    try { wsState.ws.close(1000, 'loadout change'); } catch {}
  }
  wsState.ws = null;
  wsState.connected = false;
}

function scheduleStreamReconnect(symbols) {
  if (wsState.reconnectTimer) clearTimeout(wsState.reconnectTimer);
  wsState.reconnectTimer = setTimeout(() => {
    wsState.ws = null;
    wsState.connected = false;
    wsState.key = '';
    ensureTradeStreams(isAllSymbolsRequest(symbols) ? [ALL_SYMBOLS_SENTINEL] : (config.symbols || symbols));
  }, WS_RECONNECT_MS);
}

function ingestMiniTickerArray(items) {
  if (!Array.isArray(items) || wsState.mode !== 'all') return;
  const candidates = [];
  for (const item of items) {
    const symbol = String(item.s || '').toUpperCase();
    if (!/^[A-Z0-9]{2,30}USDT$/.test(symbol)) continue;
    if (validSymbols.size && !validSymbols.has(symbol)) continue;
    wsState.discoveredSymbols.add(symbol);
    const price = Number(item.c || item.p || item.w || 0);
    if (Number.isFinite(price) && price > 0) currentPrices.set(symbol, { price, timestamp: Number(item.E || Date.now()) });
    candidates.push(symbol);
  }
  if (candidates.length) subscribeAggTradeSymbols(candidates, 'miniTicker-discovery');
}

function subscribeAllKnownSymbols(reason = 'all-known') {
  if (!wsState.ws || wsState.ws.readyState !== WebSocket.OPEN || wsState.mode !== 'all') return;
  const source = validSymbols.size ? [...validSymbols] : [...wsState.discoveredSymbols];
  subscribeAggTradeSymbols(source, reason);
}

function subscribeAggTradeSymbols(symbols, reason = 'subscribe') {
  if (!wsState.ws || wsState.ws.readyState !== WebSocket.OPEN) return;
  const clean = [...new Set((symbols || [])
    .map(s => String(s || '').trim().toUpperCase())
    .filter(s => /^[A-Z0-9]{2,30}USDT$/.test(s))
  )]
    .filter(s => !wsState.subscribedSymbols.has(s))
    .slice(0, Math.max(0, MAX_SYMBOLS - wsState.subscribedSymbols.size));

  if (!clean.length) return;
  for (const symbol of clean) {
    wsState.subscribedSymbols.add(symbol);
    wsState.discoveredSymbols.add(symbol);
    if (!tradeBuffers.has(symbol)) tradeBuffers.set(symbol, []);
  }

  const streams = clean.map(symbol => `${symbol.toLowerCase()}@aggTrade`);
  const chunks = [];
  for (let i = 0; i < streams.length; i += WS_SUBSCRIBE_CHUNK) chunks.push(streams.slice(i, i + WS_SUBSCRIBE_CHUNK));

  chunks.forEach((chunk, index) => {
    setTimeout(() => {
      if (!wsState.ws || wsState.ws.readyState !== WebSocket.OPEN) return;
      try {
        wsState.ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: chunk, id: Date.now() % 1000000000 + index }));
      } catch (error) {
        log('api-error', `WebSocket subscribe failed: ${error.message || error}`);
      }
    }, index * 180);
  });

  log('stream', `Subscribed ${clean.length} BTCUSDT aggTrade stream(s) from ${reason}. Bitcoin-only coverage active.`);
  broadcast('stream', {
    connected: wsState.connected,
    mode: wsState.mode,
    activeSymbolCount: getScanSymbols(config).length,
    subscribedSymbols: wsState.subscribedSymbols.size,
    discoveredSymbols: wsState.discoveredSymbols.size,
    timestamp: Date.now()
  });
}

function ingestAggTrade(event) {
  const symbol = String(event.s || '').toUpperCase();
  if (!symbol || symbol !== BTC_ONLY_SYMBOL) return;
  const price = Number(event.p);
  const qty = Number(event.q);
  const time = Number(event.T || event.E || Date.now());
  if (!Number.isFinite(price) || !Number.isFinite(qty) || price <= 0 || qty <= 0) return;

  const trade = {
    time,
    price,
    qty,
    buyerAggressive: event.m === false
  };
  const buffer = tradeBuffers.get(symbol) || [];
  buffer.push(trade);
  const cutoff = Date.now() - TRADE_BUFFER_MAX_MS;
  while (buffer.length && buffer[0].time < cutoff) buffer.shift();
  tradeBuffers.set(symbol, buffer);
  currentPrices.set(symbol, { price, timestamp: time });
  wsState.discoveredSymbols.add(symbol);
  wsState.lastMessageAt = Date.now();
}

async function binanceJson(pathname) {
  const url = `${BINANCE_BASE}${pathname}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      headers: {
        Accept: 'application/json',
        'User-Agent': 'WhaleHunterRadar/4.0 autonomous-bot-arena-monitoring-only'
      },
      signal: controller.signal
    });

    const text = await response.text();
    let body;
    try { body = text ? JSON.parse(text) : null; } catch { body = text; }

    if (!response.ok) {
      const msg = typeof body === 'object' && body?.msg ? body.msg : response.statusText;
      const error = new Error(`Binance HTTP ${response.status}: ${msg}`);
      error.code = 'BINANCE_HTTP_ERROR';
      throw error;
    }

    return body;
  } catch (error) {
    if (error.name === 'AbortError') {
      const timeoutError = new Error(`Binance request timed out after ${REQUEST_TIMEOUT_MS}ms.`);
      timeoutError.code = 'BINANCE_TIMEOUT';
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function sendSse(client, event, payload) {
  try {
    client.res.write(`event: ${event}\n`);
    client.res.write(`data: ${JSON.stringify(payload)}\n\n`);
  } catch {
    sseClients.delete(client);
  }
}

function broadcast(event, payload) {
  for (const client of sseClients) sendSse(client, event, payload);
}

function log(type, message) {
  const entry = { type, message, timestamp: Date.now() };
  logBuffer.push(entry);
  if (logBuffer.length > 240) logBuffer = logBuffer.slice(-240);
  console.log(`[${new Date(entry.timestamp).toISOString()}] [${type}] ${message}`);
  broadcast('log', entry);
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function clampNumber(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return clamp(n, min, max);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatUsd(value) {
  return Number(value || 0).toLocaleString('en-US', { maximumFractionDigits: 0 });
}

function formatDuration(seconds) {
  const s = Number(seconds) || 0;
  if (s >= 3600 && s % 3600 === 0) return `${s / 3600}h`;
  if (s >= 60 && s % 60 === 0) return `${s / 60}m`;
  return `${s}s`;
}

app.listen(PORT, () => {
  startBotLoop();
  startLiveHiveLoop();
  startScientistThinkLoop();
  ensureTradeStreams(config.symbols);
  initValidSymbols();
  if (AUTOSTART) {
    running = true;
    scheduleNextScan(9000);
  }
  log('server', `Whale Hunter Bitcoin Two Formula Only V17 online on port ${PORT}. BTCUSDT only, exactly two locked formulas, fake-money paper arena. Monitoring only. No API keys. No real trading.`);
  broadcast('status', { running, scanning, autostart: AUTOSTART, timestamp: Date.now() });
});
