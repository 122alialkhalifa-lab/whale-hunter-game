# Whale Hunter Radar — Autonomous Bot Arena V4

لعبة مراقبة حيتان بأسلوب Game HUD للآيباد، مبنية على Node.js backend وواجهة PWA.

## الفكرة

هذه النسخة تجعل البطولة **روبوتات آلية بالكامل** وليست لاعبين بشر:

- 500 Bot متسابق داخل الرادار.
- كل Bot له مدرسة قراءة مختلفة: Big Buy، Buy Imbalance، Absorption، Net Whale، Sell Pressure، Momentum، Range Compression وغيرها.
- الروبوتات تتحرك تلقائيًا على الرادار كل ثانية تقريبًا عبر SSE `bot-tick`.
- إذا قرأ Bot إشارة شراء، يتحرك نحو الهدف كـ `CHASING` أو `BOSS_RUSH`.
- إذا قرأ ضغط بيع، يتحول إلى `RED_TIDE`.
- إذا لم يجد قراءة واضحة، يدور حول الهدف أو يعمل `SCOUTING / PATROL`.
- بعد مدة التوقع، اللعبة تقارن سعر البداية بسعر التقييم وتمنح XP للروبوتات حسب دقة القراءة.

هذه لعبة مراقبة وتقييم قراءة فقط. لا توجد صفقات، ولا أوامر، ولا API keys.

## التشغيل

```bash
npm install
npm start
```

افتح:

```text
http://localhost:3000
```

من الآيباد افتح الرابط من Safari ثم Share ثم Add to Home Screen.

## الملفات

```text
package.json
server.mjs
public/index.html
public/manifest.json
public/sw.js
public/icon.svg
```

## Backend endpoints

- `GET /` — الواجهة.
- `GET /events` — Server-Sent Events، ومنها أحداث `scan`, `arena`, `bot-tick`.
- `POST /config` — حفظ الإعدادات.
- `POST /start` — بدء الصيد المتكرر.
- `POST /stop` — إيقاف الصيد.
- `POST /scan` — Sonar Pulse فوري.
- `GET /health` — صحة السيرفر + حالة الروبوتات.
- `GET /arena` — حالة بطولة الـ 500 Bot.
- `POST /arena/reset` — إعادة ضبط البطولة داخل الذاكرة.

## Binance data

السيرفر فقط يتصل بـ Binance public endpoints. المتصفح/الآيباد لا يتصل مباشرة بـ Binance.

- `GET /fapi/v1/exchangeInfo`
- `GET /fapi/v1/aggTrades?symbol=SYMBOL&limit=1000`

لا يحتاج API key.

## Safety

- Monitoring only.
- No trading.
- No auto-buy.
- No auto-sell.
- No Binance API keys.
- No guaranteed profit claims.

الرسالة الظاهرة في التطبيق:

> مراقبة فقط. ليست توصية مالية. الرافعة قد تصفّر الحساب.

الإشارات داخل التطبيق تعني:

- possible whale footprint
- watch signal

ولا تعني دخول مضمون أو توصية مالية.
