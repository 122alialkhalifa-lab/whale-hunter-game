import express from 'express';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const BINANCE_BASE = 'https://fapi.binance.com';
const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 35);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 9000);
const SYMBOL_DELAY_MS = Number(process.env.SYMBOL_DELAY_MS || 140);
const CONTESTANT_COUNT = 500;
const BOT_TICK_MS = Number(process.env.BOT_TICK_MS || 1100);

const defaultConfig = Object.freeze({
  symbols: [
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT',
    'LINKUSDT', 'AVAXUSDT', 'SUIUSDT', 'ONDOUSDT', 'VETUSDT',
    'HBARUSDT', 'PEPEUSDT', 'WIFUSDT'
  ],
  whaleUsd: 30000,
  minScore: 70,
  windowSec: 60,
  intervalSec: 8,
  arenaHorizonSec: 3600
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
let resolvedPredictions = [];
let activePredictions = [];
let botTimer = null;
let lastBotTickAt = Date.now();
const contestants = createContestants(CONTESTANT_COUNT);

app.disable('x-powered-by');
app.use(express.json({ limit: '96kb' }));
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '2h',
  setHeaders(res, filePath) {
    if (filePath.endsWith('sw.js')) res.setHeader('Cache-Control', 'no-cache');
  }
}));

app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    running,
    scanning,
    timestamp: Date.now(),
    config,
    arena: getArenaSnapshot(),
    botSwarm: getBotSwarm(),
    exchangeInfoLoadedAt,
    validSymbolCount: validSymbols.size,
    lastScanAt: lastScan?.timestamp ?? null
  });
});

app.get('/arena', (_req, res) => {
  const arena = getArenaSnapshot();
  res.json({ ok: true, arena, botSwarm: arena.botSwarm });
});

app.post('/arena/reset', (_req, res) => {
  resetArena();
  const arena = getArenaSnapshot();
  log('arena', 'Prediction Arena reset: 500 contestants are ready for a new tournament.');
  broadcast('arena', arena);
  res.json({ ok: true, arena });
});

app.get('/events', (req, res) => {
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
    scanning,
    timestamp: Date.now(),
    config,
    arena: getArenaSnapshot(),
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

app.post('/config', async (req, res) => {
  const incoming = req.body || {};
  const normalized = await normalizeConfig({ ...config, ...incoming });
  config = normalized.config;
  log('loadout', `Loadout saved: ${config.symbols.length} symbols, whale $${formatUsd(config.whaleUsd)}, minScore ${config.minScore}, arena ${formatDuration(config.arenaHorizonSec)}.`);
  broadcast('config', { config, rejected: normalized.rejected, warnings: normalized.warnings, timestamp: Date.now() });
  if (running) scheduleNextScan(100);
  res.json({ ok: true, config, rejected: normalized.rejected, warnings: normalized.warnings });
});

app.post('/start', async (_req, res) => {
  if (!running) {
    running = true;
    scheduleNextScan(100);
    log('engine', `Hunt started. Scan interval: ${config.intervalSec}s. Arena horizon: ${formatDuration(config.arenaHorizonSec)}.`);
    broadcast('status', { running, scanning, timestamp: Date.now() });
  }
  res.json({ ok: true, running, config, arena: getArenaSnapshot() });
});

app.post('/stop', (_req, res) => {
  running = false;
  if (scanTimer) clearTimeout(scanTimer);
  scanTimer = null;
  log('engine', 'Hunt stopped. Radar is idle. Open arena predictions stay in memory until the server restarts or arena reset.');
  broadcast('status', { running, scanning, timestamp: Date.now() });
  res.json({ ok: true, running });
});

app.post('/scan', async (_req, res) => {
  const result = await runScan({ manual: true });
  res.status(result.ok ? 200 : 429).json(result);
});

app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ ok: false, error: 'SERVER_ERROR', message: err.message || String(err) });
});

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
  const results = [];
  const errors = [];

  broadcast('scan-start', { timestamp: startedAt, manual, symbols: cfg.symbols });
  log('scanner', `${manual ? 'Manual scan' : 'Auto scan'} launched for ${cfg.symbols.length} sectors.`);

  for (const symbol of cfg.symbols) {
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
  const arena = updateArena(results, cfg, Date.now());
  const finishedAt = Date.now();

  lastScan = {
    ok: true,
    timestamp: finishedAt,
    durationMs: finishedAt - startedAt,
    manual,
    running,
    config: cfg,
    results,
    hotCount: hot.length,
    top,
    arena,
    errors
  };

  scanning = false;
  log('scanner', `Scan complete: ${hot.length} hot signal(s), top target ${top || 'N/A'}, ${errors.length} API error(s). Arena open: ${arena.openCount}, resolved: ${arena.resolvedCount}.`);
  broadcast('scan', lastScan);
  broadcast('arena', arena);
  broadcast('status', { running, scanning, timestamp: Date.now() });
  return lastScan;
}

async function scanSymbol(symbol, cfg) {
  const params = new URLSearchParams({ symbol, limit: '1000' });
  const trades = await binanceJson(`/fapi/v1/aggTrades?${params}`);
  if (!Array.isArray(trades)) throw new Error('Unexpected aggTrades response.');

  const now = Date.now();
  const cutoff = now - Number(cfg.windowSec) * 1000;
  const recent = trades
    .filter(t => Number(t.T) >= cutoff)
    .map(t => ({
      time: Number(t.T),
      price: Number(t.p),
      qty: Number(t.q),
      buyerAggressive: t.m === false
    }))
    .filter(t => Number.isFinite(t.price) && Number.isFinite(t.qty) && t.price > 0 && t.qty > 0)
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
  const firstPrice = recent[0]?.price ?? 0;
  const lastPrice = recent.at(-1)?.price ?? 0;
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
    tags,
    timestamp: Date.now(),
    note: score >= cfg.minScore ? 'possible whale footprint' : 'watch signal'
  };
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
  const createdThisScan = [];

  if (Array.isArray(results) && results.length) {
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

  if (createdThisScan.length) {
    log('arena', `${createdThisScan.length} contestants locked new ${formatDuration(cfg.arenaHorizonSec)} watch predictions.`);
  }
  if (resolvedThisScan.length) {
    const wins = resolvedThisScan.filter(item => item.hit).length;
    log('arena', `${resolvedThisScan.length} arena prediction(s) resolved: ${wins} hit, ${resolvedThisScan.length - wins} missed.`);
  }

  return getArenaSnapshot({ createdThisScan, resolvedThisScan });
}

function createPrediction(contestant, results, cfg, now) {
  let best = null;
  for (const signal of results) {
    if (!signal.price) continue;
    const reading = readSignal(contestant, signal, cfg);
    if (!best || reading.conviction > best.reading.conviction) best = { signal, reading };
  }
  if (!best) return null;

  const { signal, reading } = best;
  const direction = reading.direction;
  const prediction = {
    id: randomUUID(),
    contestantId: contestant.id,
    callsign: contestant.callsign,
    style: contestant.style.name,
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
    status: 'OPEN'
  };
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

  let conviction = 25;
  let direction = contestant.style.direction;
  let basis = contestant.style.desc;

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
  conviction = clamp(Math.round(conviction), 1, 100);
  if (conviction < contestant.minConviction) direction = 'WATCH';
  return { conviction, direction, basis };
}

function resolvePrediction(prediction, exitPrice, now, signal) {
  const contestant = contestants[prediction.contestantId - 1];
  const movePct = prediction.entryPrice > 0 ? ((exitPrice - prediction.entryPrice) / prediction.entryPrice) * 100 : 0;
  const threshold = prediction.direction === 'WATCH' ? 0.25 : 0.18;
  const hit = prediction.direction === 'UP'
    ? movePct >= threshold
    : prediction.direction === 'DOWN'
      ? movePct <= -threshold
      : Math.abs(movePct) <= threshold;

  const base = hit ? 12 : -6;
  const moveBonus = hit ? Math.round(Math.min(32, Math.abs(movePct) * 8)) : -Math.round(Math.min(10, Math.abs(movePct) * 2));
  const confidenceBonus = hit ? Math.round(prediction.conviction / 14) : -Math.round(Math.max(0, prediction.conviction - 70) / 18);
  const scoreDelta = base + moveBonus + confidenceBonus;

  contestant.xp = Math.max(0, contestant.xp + scoreDelta);
  contestant.pending = Math.max(0, contestant.pending - 1);
  contestant.games += 1;
  contestant.lastResult = hit ? 'HIT' : 'MISS';
  contestant.lastMovePct = movePct;
  contestant.lastSymbol = prediction.symbol;
  contestant.lastDirection = prediction.direction;
  contestant.lastResolvedAt = now;
  markBotResolved(contestant, hit, prediction, movePct, now);
  if (hit) {
    contestant.wins += 1;
    contestant.streak = Math.max(1, contestant.streak + 1);
  } else {
    contestant.losses += 1;
    contestant.streak = Math.min(-1, contestant.streak - 1);
  }
  contestant.bestStreak = Math.max(contestant.bestStreak, contestant.streak);

  return {
    ...prediction,
    status: 'RESOLVED',
    exitPrice,
    movePct,
    hit,
    scoreDelta,
    resolvedAt: now,
    resolvedSignalScore: signal?.score ?? null,
    resolvedSignalTags: signal?.tags ?? []
  };
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
      lastMovePct: contestant.lastMovePct
    }))
    .sort((a, b) => b.xp - a.xp || b.accuracy - a.accuracy || b.wins - a.wins || a.id - b.id);

  const activeTop = activePredictions
    .slice()
    .sort((a, b) => b.conviction - a.conviction || b.signalScore - a.signalScore)
    .slice(0, 20)
    .map(pred => ({
      ...pred,
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
      games: 0
    };
    group.racers += 1;
    group.xp += contestant.xp;
    group.wins += contestant.wins;
    group.games += contestant.games;
    groups.set(key, group);
  }
  return [...groups.values()]
    .map(group => ({
      ...group,
      avgXp: group.racers ? group.xp / group.racers : 0,
      accuracy: group.games ? group.wins / group.games : 0
    }))
    .sort((a, b) => b.avgXp - a.avgXp || b.accuracy - a.accuracy)
    .slice(0, 10);
}

function resetArena() {
  activePredictions = [];
  resolvedPredictions = [];
  currentPrices = new Map();
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
    resetBotState(contestant, Date.now());
  }
}

function createContestants(count) {
  const prefixes = ['Radar', 'Sonar', 'Neon', 'Abyss', 'Pulse', 'Tide', 'Hydra', 'Orbit', 'Cyber', 'Depth', 'Falcon', 'Kraken'];
  const arabic = ['الغواص', 'الصياد', 'المرصاد', 'النورس', 'القبطان', 'القناص', 'الحارس', 'الملاح', 'الرادار', 'الكشاف'];
  return Array.from({ length: count }, (_, idx) => {
    const id = idx + 1;
    const style = strategyDeck[idx % strategyDeck.length];
    const prefix = prefixes[idx % prefixes.length];
    const ar = arabic[idx % arabic.length];
    return {
      id,
      callsign: `${prefix}-${String(id).padStart(3, '0')} ${ar}`,
      style,
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

function markBotResolved(contestant, hit, prediction, movePct, now) {
  const bot = contestant.bot || (contestant.bot = createBotState(contestant.id, contestant.style));
  bot.mode = hit ? 'CELEBRATE' : 'RECALIBRATE';
  bot.stateTag = hit ? `HIT ${movePct.toFixed(2)}%` : `MISS ${movePct.toFixed(2)}%`;
  bot.targetSymbol = prediction.symbol;
  bot.direction = prediction.direction;
  bot.energy = clamp(bot.energy + (hit ? 18 : -13), 18, 100);
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

async function initValidSymbols() {
  try {
    log('api', 'Loading Binance USDⓈ-M Futures exchangeInfo for symbol validation.');
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

    log('api', `Validated ${validSymbols.size} Binance USDT perpetual symbols. Active loadout: ${config.symbols.length}.`);
    if (normalized.rejected.length) {
      log('loadout', `Rejected invalid/default symbols: ${normalized.rejected.join(', ')}`);
    }
    broadcast('config', { config, rejected: normalized.rejected, warnings: normalized.warnings, timestamp: Date.now() });
  } catch (error) {
    validSymbols = new Set();
    exchangeInfoLoadedAt = null;
    log('api-error', `exchangeInfo validation failed: ${error.message || error}. The server will retry on config save/startup restart.`);
  }
}

async function normalizeConfig(raw) {
  const warnings = [];
  const rejected = [];

  let symbols = Array.isArray(raw.symbols)
    ? raw.symbols
    : String(raw.symbols || '').split(/[\s,]+/);

  symbols = [...new Set(symbols
    .map(s => String(s || '').trim().toUpperCase())
    .filter(Boolean)
  )];

  if (symbols.length > MAX_SYMBOLS) {
    warnings.push(`Symbol list trimmed to max ${MAX_SYMBOLS}.`);
    symbols = symbols.slice(0, MAX_SYMBOLS);
  }

  if (validSymbols.size > 0) {
    const before = symbols;
    symbols = before.filter(s => validSymbols.has(s));
    rejected.push(...before.filter(s => !validSymbols.has(s)));
  } else {
    warnings.push('Symbol validation is temporarily unavailable because exchangeInfo could not be loaded.');
  }

  if (symbols.length === 0) {
    symbols = defaultConfig.symbols.slice(0, MAX_SYMBOLS);
    warnings.push('No valid symbols supplied; fallback loadout restored.');
  }

  const whaleUsd = clampNumber(raw.whaleUsd, 100, 10000000, defaultConfig.whaleUsd);
  const minScore = clampNumber(raw.minScore, 1, 100, defaultConfig.minScore);
  const windowSec = clampNumber(raw.windowSec, 10, 600, defaultConfig.windowSec);
  const intervalSec = clampNumber(raw.intervalSec, 3, 300, defaultConfig.intervalSec);
  const arenaHorizonSec = clampNumber(raw.arenaHorizonSec, 60, 14400, defaultConfig.arenaHorizonSec);

  return {
    config: {
      symbols,
      whaleUsd,
      minScore,
      windowSec,
      intervalSec,
      arenaHorizonSec
    },
    rejected,
    warnings
  };
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
  log('server', `Whale Hunter Radar Bot Arena online on port ${PORT}. 500 autonomous robot contestants. Monitoring only. No API keys. No trading.`);
  initValidSymbols();
});
