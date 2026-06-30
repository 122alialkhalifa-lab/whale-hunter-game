# Whale Hunter Radar — Bitcoin Two Formula Only V17

نسخة BTCUSDT فقط، مقفلة على معادلتين فقط كما طلبت. لا يوجد أي نموذج ثالث ولا تجربة استراتيجية جديدة.

## المعادلتان المسموحتان حرفيًا

### 1) BTCUSDT DOWN — DOGFIGHT — 8x

```text
[2026-06-30T22:38:22.424Z] BTCUSDT DOWN | Radar-013 المرصاد DOGFIGHT | WIN net=0.5379% gross=1.4970% fees+slip=0.9591% move=-0.1871% lev=8x | pattern=1/1 PERFECT_SO_FAR | equation: gross=1.4970% - fees=0.6394% - slippage=0.3197% => net=0.5379%; result=WIN; R=0.7250; Δwᵢ=η·R·xᵢ
```

### 2) BTCUSDT UP — TAKER_BUY — 12.3x

```text
[2026-06-30T22:08:20.304Z] BTCUSDT UP | Radar-001 الغواص TAKER_BUY | WIN net=0.6171% gross=2.0943% fees+slip=1.4772% move=0.1703% lev=12.3x | pattern=1/1 PERFECT_SO_FAR | equation: gross=2.0943% - fees=0.9848% - slippage=0.4924% => net=0.6171%; result=WIN; R=0.7874; Δwᵢ=η·R·xᵢ
```

## منطق التداول الوهمي

- اللعبة تستخدم BTCUSDT فقط.
- السيرفر يقرأ Binance Futures WebSocket الحقيقي لـ `btcusdt@aggTrade`.
- التداول وهمي بالكامل داخل اللعبة.
- لا API keys.
- لا أوامر شراء أو بيع حقيقية.
- لا يوجد auto-trading حقيقي.
- كل جولة تفتح فقط صيغتين وهميتين: UP/TAKER_BUY و DOWN/DOGFIGHT.
- الرافعة ثابتة حسب المعادلة:
  - UP = 12.3x
  - DOWN = 8x
- التقييم صار Strict:
  - WATCH لا يحسب win.
  - الربح لا يحسب win إلا بعد الرسوم والانزلاق.
  - `R = 0` إذا النتيجة NEUTRAL أو WATCH.
  - `TRADE_PROOF = real_wins / real_closed_trials` فقط.

## تشغيل

```bash
npm install
npm start
```

## Render

Build Command:

```bash
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
```

Start Command:

```bash
node server.mjs
```

## Access

Default owner PIN:

```text
OWNER-7777
```

Default viewer PIN:

```text
VIEW-1111
```

غيّرها من Render Environment Variables:

```text
OWNER_PIN=your-owner-pin
VIEWER_PIN=your-viewer-pin
AUTH_SECRET=long-secret
```

## Health

```text
/health
```

لازم تشوف:

```json
{"ok":true,"websocketConnected":true,"twoFormulaOnlyMode":true}
```

## Safety

مراقبة وتداول وهمي فقط. ليست توصية مالية. الرافعة قد تصفّر الحساب الحقيقي.
