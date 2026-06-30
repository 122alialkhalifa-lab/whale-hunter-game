# Whale Hunter Radar — Hive Mind Paper Arena V8

لعبة مراقبة حيتان بأسلوب **خلية نحل ذكية** على الآيباد.

## الفكرة

- تراقب كل رموز Binance USDⓈ-M Futures التي تنتهي بـ USDT عبر WebSocket.
- 500 Bot داخل اللعبة، كل واحد يبدأ بـ **$1000 وهمية**.
- كل Bot له منهج قراءة مختلف: Big Buy, Buy Imbalance, Absorption, Sell Pressure, Momentum وغيرها.
- الأموال والصفقات **وهمية بالكامل** داخل اللعبة فقط.
- الأسعار والمؤشرات والتدفق تأتي من بيانات Binance Futures العامة.
- كل Bot إذا ربح يحفظ السبب ويكرر نفس النمط عند ظهوره.
- كل Bot إذا خسر يحلل سبب الخسارة: ضغط بيع أقوى، فخ، دخول متأخر، عدم استمرار الحركة، إلخ.
- الخلية تشارك المعلومات بين الروبوتات عبر Hive Mind:
  - Shared Winning Rules
  - Loss Reviews
  - Coin Memory
  - Pattern Brain

## أمان

لا يوجد تداول حقيقي.
لا توجد أوامر شراء أو بيع.
لا توجد Binance API keys.
لا توجد مفاتيح خاصة.
لا توجد توصيات دخول مضمونة.

الإشارات داخل اللعبة تعني:

- possible whale footprint
- watch signal

وليست توصية مالية. الرافعة قد تصفّر الحساب.

## التشغيل المحلي

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

## إعدادات مقترحة

في Symbols اترك:

```text
ALL_BINANCE_USDT
```

حتى تراقب كل عملات Binance Futures USDT المتاحة من WebSocket.

## ملاحظة عن الذاكرة

Hive Mind تحفظ التعلم في ذاكرة السيرفر أثناء تشغيله. إذا نام Render Free أو أعيد تشغيل السيرفر، تبدأ الخلية من جديد.
