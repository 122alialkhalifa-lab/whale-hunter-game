# Whale Hunter Quant Proof V13

لعبة/مختبر مراقبة رياضي على الآيباد. كل عملة من أكبر عملات Binance USDⓈ-M Futures تعتبر مسألة رياضية مستقلة، و500 روبوت علماء يحاولون اكتشاف أنماط صعود/نزول متكررة النجاح.

## المهم

- مراقبة وتداول وهمي فقط.
- لا Binance API keys.
- لا شراء حقيقي ولا بيع حقيقي.
- الأسعار والمؤشرات من Binance Futures public WebSocket.
- التنبيه القوي `PERFECT-SO-FAR` يعني أن النمط لم يفشل داخل سجل اللعبة حتى الآن بعد وصوله إلى عدد تجارب كافٍ. هذا ليس ضمانًا للمستقبل.

## الجديد في V13

- لوحة دخول بصلاحيات.
- Owner: تشغيل/إيقاف/تعديل/تصدير/تصفير المختبر.
- Viewer: مشاهدة فقط بدون تحكم.
- `/health` خفيف ومناسب لمواقع ping.
- `/proof` يعرض أنماط 100% داخل السجل.
- `Coin Proof Lab`: مستوى فهم كل عملة.
- `Pattern Vault`: سجل نجاح/فشل المعادلات.
- `Live Math Reports`: تقارير الروبوتات ومعادلاتها لحظيًا.

## التشغيل

```bash
npm install
npm start
```

ثم افتح:

```text
http://localhost:3000
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

Region المفضل إذا Binance حظر أمريكا: Singapore.

## صلاحيات الدخول

يفضل تغيير الأكواد من Render Environment Variables:

```text
OWNER_PIN=your-owner-pin
VIEWER_PIN=your-viewer-pin
AUTH_SECRET=long-random-secret
MIN_PROOF_SAMPLES=25
```

الأكواد الافتراضية للتجربة فقط:

```text
OWNER-7777
VIEW-1111
```

## Endpoints

Public:

```text
GET /health
POST /auth/login
POST /auth/logout
GET /auth/me
```

Viewer/Owner:

```text
GET /events
GET /state
GET /hive
GET /arena
GET /formulas
GET /proof
```

Owner only:

```text
POST /start
POST /stop
POST /scan
POST /config
POST /arena/reset
POST /admin/reset-lab
GET /admin/export
```

## تنبيه السلامة

مراقبة فقط. ليست توصية مالية. الرافعة قد تصفّر الحساب. كل الأموال والصفقات داخل اللعبة وهمية.
