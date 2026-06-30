# Whale Hunter Radar — Autonomous Bot Arena V7

نسخة iPad Easy + All Binance Mode.

- تراقب كل رموز Binance USDⓈ-M Futures التي تنتهي بـ USDT عبر WebSocket.
- لا تستخدم REST scans لكل عملة، لتقليل أخطاء 418.
- تدعم `ALL_BINANCE_USDT` في خانة Symbols.
- 500 روبوت Bot يتحركون تلقائيًا داخل الرادار.
- مراقبة فقط: لا تداول، لا API keys، لا شراء، لا بيع.

## التشغيل

```bash
npm install
npm start
```

افتح:

```text
http://localhost:3000
```

## Render Build Command

```bash
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
```

## Render Start Command

```bash
node server.mjs
```

## ملاحظة

إذا كتبت في Symbols:

```text
ALL_BINANCE_USDT
```

السيرفر يكتشف رموز Binance Futures USDT تلقائيًا من WebSocket `!miniTicker@arr` ثم يشترك في `@aggTrade` لها.

الإشارات تعني possible whale footprint / watch signal فقط. ليست توصية مالية. الرافعة قد تصفّر الحساب.
