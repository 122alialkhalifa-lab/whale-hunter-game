# Whale Hunter Bitcoin Quant Proof V16

Bitcoin-only edition.

Everything is locked to **BTCUSDT**:

- 500 scientist bots
- paper/fake $1000 balances
- fake leverage simulation
- mathematical pattern proof engine
- Perfect-So-Far alerts
- Successful Equation Logs
- dashboard access permissions
- `/formula-success-logs` and `/formula-success-logs.txt`

Market data source:

- Real Binance USDⓈ-M Futures WebSocket data for `BTCUSDT@aggTrade`
- Prices, trade flow, buy/sell pressure, and indicators are calculated from real public market data.
- Paper positions, PnL, fake USD, and leverage are simulation only.

## Login codes

Default owner code:

```text
OWNER-7777
```

Default viewer code:

```text
VIEW-1111
```

Change them in Render Environment Variables:

```text
OWNER_PIN=your-owner-code
VIEWER_PIN=your-view-code
AUTH_SECRET=long-random-secret
MIN_PROOF_SAMPLES=25
SUCCESS_FORMULA_LOG_MAX=1500
```

## Deploy on Render

Build Command:

```bash
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
```

Start Command:

```bash
node server.mjs
```

## Health check

Use this for cron-job.org or uptime ping:

```text
/health
```

You want to see:

```json
{ "ok": true, "websocketConnected": true }
```

## Safety

- Monitoring and paper-simulation only.
- No Binance API keys.
- No real buy/sell orders.
- No guaranteed profit claims.
- Visible warning remains: leverage can wipe accounts.
- `100%` means perfect inside the app's recorded paper-simulation history so far, not guaranteed future profit.

## V16 Strict Evaluation Fixes

This version fixes the evaluation logic, not only the message text.

- `WATCH_SCORE` is separated from `TRADE_PROOF`.
- WATCH-only decisions never count as wins.
- A paper trade only becomes `WIN` when net PnL after fees and slippage is greater than `MIN_NET_PROFIT_PCT`.
- Neutral trades are closed trials but not wins.
- `PERFECT-SO-FAR` means `wins === real_closed_trials` with zero losses and zero neutral trials after the minimum sample count.
- Learning reward uses strict net result:

```text
if result == WIN:  R = abs(move) + abs(netPnL%)
if result == LOSS: R = -(abs(move) + abs(netPnL%))
else:              R = 0
```

- Duplicate BTCUSDT setups with the same timeframe, direction, feature vector, and time bucket are not counted repeatedly in proof stats.

Optional Render Environment Variables:

```text
PAPER_FEE_BPS=4
PAPER_SLIPPAGE_BPS=2
MIN_NET_PROFIT_PCT=0.05
MAX_NEUTRAL_LOSS_PCT=0.05
```
