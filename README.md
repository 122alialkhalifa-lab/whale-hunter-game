# Whale Hunter Radar — Bitcoin Two Formula Fingerprint V18

نسخة Bitcoin-only مبنية على فكرتك الأخيرة:

- المعادلتان الأصليتان محفوظتان حرفيًا كـ **Reference Winning Formulas**.
- لا يتم التداول الوهمي بمجرد وجود المعادلة.
- يتم التداول الوهمي فقط إذا السوق الحالي يشبه بصمة صفقة الربح الأصلية بنسبة عالية.
- الافتراضي: `FORMULA_FINGERPRINT_MIN_SIMILARITY=90`.
- إذا التشابه أقل من 90%، تعرض اللعبة السبب ولا تفتح صفقة وهمية.
- إذا تشابه UP و DOWN في نفس اللحظة، يتم اختيار الأعلى تشابهًا فقط لتجنب Long/Short عكسيين على نفس BTC.

## الصيغة الصحيحة

المعادلتان اللتان زودتني بهما تبقيان كما هما للتقييم بعد الصفقة:

```text
BTCUSDT UP | Radar-001 الغواص TAKER_BUY
WIN net=0.6171% gross=2.0943% fees+slip=1.4772% move=0.1703% lev=12.3x
result=WIN; R=0.7874; Δwᵢ=η·R·xᵢ
```

```text
BTCUSDT DOWN | Radar-013 المرصاد DOGFIGHT
WIN net=0.5379% gross=1.4970% fees+slip=0.9591% move=-0.1871% lev=8x
result=WIN; R=0.7250; Δwᵢ=η·R·xᵢ
```

لكن الدخول الآن يعتمد على:

```text
similarity(current_market, reference_fingerprint) >= FORMULA_FINGERPRINT_MIN_SIMILARITY
```

## بيانات Binance

- السعر والحركة والتدفق من Binance USDⓈ-M Futures WebSocket الحقيقي.
- يستخدم BTCUSDT فقط.
- لا Binance API keys.
- لا أوامر شراء أو بيع حقيقية.
- التداول، الرافعة، الأرباح والخسائر كلها وهمية داخل اللعبة.

## Environment Variables اختيارية

```text
OWNER_PIN=OWNER-7777
VIEWER_PIN=VIEW-1111
AUTH_SECRET=change-this-secret-in-render
FORMULA_FINGERPRINT_MIN_SIMILARITY=90
PAPER_FEE_BPS=4
PAPER_SLIPPAGE_BPS=2
MIN_NET_PROFIT_PCT=0.05
MAX_NEUTRAL_LOSS_PCT=0.05
```

## تشغيل Render

Build Command:

```bash
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
```

Start Command:

```bash
node server.mjs
```

## فحص

افتح:

```text
/health
```

لازم تشوف:

```json
"ok": true,
"twoFormulaOnlyMode": true,
"websocketConnected": true,
"formulaFingerprintMinSimilarity": 90
```

