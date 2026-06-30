# Whale Hunter Quant Proof V14

Arabic/English game-style Binance USDⓈ-M Futures monitoring lab.

V14 adds a **Successful Equation Logs** dashboard:

- Every winning paper-trade equation is saved as a copyable log.
- Dashboard panel: `Successful Equation Logs / سجل المعادلات الناجحة`.
- One-click copy for each equation.
- `COPY ALL` copies the filtered successful equations.
- `OPEN TXT` opens a plain-text log you can select/copy/share.
- API endpoints:
  - `GET /formula-success-logs`
  - `GET /formula-success-logs.txt`
- Owner export now includes `successfulFormulaLogs`.

Important: these logs are paper-simulation records only. Prices and market data are real Binance public futures data, but no real orders are placed.

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

## Safety

- Monitoring and paper-simulation only.
- No Binance API keys.
- No real buy/sell orders.
- No guaranteed profit claims.
- Visible warning remains: leverage can wipe accounts.
