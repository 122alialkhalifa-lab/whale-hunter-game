# Whale Hunter Bitcoin Quant Proof V15

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
