# Whale Hunter Radar — Bitcoin Collective Formula Completion + Live Data Guard V21

نسخة BTCUSDT فقط. هذه النسخة لا تستخدم المعادلتين كأوامر دخول مباشرة، بل تستخدمهما كبذور رياضية ناقصة. كل Bot يحاول إضافة قطعة من المعادلة حتى يرتفع التشابه تدريجيًا من 17–25% إلى 50% ثم 70% ثم 90%+ من خلال تجارب Paper Trading حية على بيانات Binance Futures.

## الفكرة

- BTCUSDT فقط.
- 500 Bot.
- كل Bot يبدأ بـ 1,000,000 دولار وهمية.
- إجمالي رأس المال الوهمي: 500,000,000 دولار.
- الأسعار والصفقات من Binance Futures WebSocket: `btcusdt@aggTrade`.
- التداول والرافعة والربح والخسارة وهمية فقط.
- لا API keys.
- لا شراء حقيقي.
- لا بيع حقيقي.

## المنطق الجديد

المعادلتان الأصليتان محفوظتان حرفيًا كبذور:

1. Radar-001 TAKER_BUY UP
2. Radar-013 DOGFIGHT DOWN

لكن اللعبة لا تدخل لأن المعادلة موجودة. اللعبة تجعل الروبوتات تسأل:

- ما القطعة الناقصة التي ترفع التشابه؟
- هل TAKER_BUY مهم؟
- هل RANGE_LOCK مهم؟
- هل BIG_SELL هو سبب الفشل؟
- هل TIME / FLOW / NET_WHALE تكمل المعادلة؟

كل Bot يقترح Patch رياضي، يفتح تجربة وهمية، ثم النتيجة إما:

- WIN صافي بعد الرسوم والانزلاق: تثبيت القطع ورفع الأوزان.
- LOSS: تخفيض الأوزان ووسم المعادلة BROKEN.
- NEUTRAL: لا تُحسب كفوز.

## التقييم الصارم

```
move = (exit - entry) / entry * 100
LONG gross = move * leverage
SHORT gross = -move * leverage
fees = roundtrip fee bps * leverage
slippage = roundtrip slippage bps * leverage
net = gross - fees - slippage

if net > MIN_NET_PROFIT_PCT => WIN
elif net < -MAX_NEUTRAL_LOSS_PCT => LOSS
else => NEUTRAL

R = abs(move)+abs(net) إذا WIN
R = -(abs(move)+abs(net)) إذا LOSS
R = 0 إذا NEUTRAL
```

## الإعدادات المهمة في Render

Build Command:

```bash
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
```

Start Command:

```bash
node server.mjs
```

Environment Variables اختيارية:

```text
OWNER_PIN=OWNER-7777
VIEWER_PIN=VIEW-1111
AUTH_SECRET=change-this-secret
INITIAL_FAKE_USD=1000000
BOT_COUNT=500
TRADE_HORIZON_SEC=600
MAX_FAKE_LEVERAGE=20
MIN_NET_PROFIT_PCT=0.05
PAPER_FEE_BPS=4
PAPER_SLIPPAGE_BPS=2
```

## روابط مهمة

- `/health` فحص سريع.
- `/formula-success-logs` سجل JSON للمعادلات الرابحة.
- `/formula-success-logs.txt` سجل نصي سهل النسخ.

## الدخول الافتراضي

مالك:

```text
OWNER-7777
```

مشاهد:

```text
VIEW-1111
```

## أمان

هذه لعبة مراقبة وتجارب وهمية فقط. لا توجد مفاتيح Binance ولا أوامر تداول حقيقية ولا ادعاء ضمان ربح.


## V21 Live Data Guard

هذه النسخة لا تكتفي بـ websocketConnected. تضيف lastTradeAt, lastTradeAgeMs, lastPriceSource, tradeMessageCount, wsMessageCount, dataWarning، وتستخدم fallback خفيف إذا كان الاتصال مفتوحًا لكن لا تصل صفقات BTC.
