# Whale Hunter Radar — Hive Scientists V10

لعبة مراقبة حيتان بأسلوب Game HUD للآيباد. هذه النسخة تجعل الـ 500 Bot يعملون مثل **علماء رياضيات داخل خلية نحل**:

- كل Bot يبدأ بـ `$1000` وهمية فقط.
- الأسعار والمؤشرات من Binance USDⓈ-M Futures public WebSocket.
- لا يوجد تداول حقيقي، لا API keys، لا أوامر شراء أو بيع.
- كل Bot يفتح قرارات/مراكز وهمية داخل اللعبة فقط.
- كل Bot يرسل **معادلة قرار** لحظيًا:

```text
C = 50 + 38Σ(wᵢ·xᵢ) + H
```

- بعد الربح أو الخسارة يرسل **برهانًا/تشريحًا رياضيًا**:

```text
R = sign(PnL)·(|move|+|PnL|)
Δwᵢ = η·R·xᵢ
```

- كل الروبوتات تتعلم من التقرير فورًا عبر Hive Mind.
- الرابح يرفع وزن المؤشرات التي سبقت النجاح.
- الخاسر يخفض أو يعكس وزن المؤشرات التي سبقت الفشل.
- الواجهة تعرض:
  - Live Math Reports
  - Collective Equation
  - Shared Winning Rules
  - Loss Reviews
  - Coin Memory
  - Pattern Brain

## التشغيل

```bash
npm install
npm start
```

افتح:

```text
http://localhost:3000
```

## على Render

Build Command:

```bash
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
```

Start Command:

```bash
node server.mjs
```

يفضل Region: Singapore.

## ملاحظات أمان

مراقبة ومحاكاة فقط. ليست توصية مالية. الرافعة قد تصفّر الحساب.
لا توجد مفاتيح Binance، لا تنفيذ صفقات، لا auto-buy، لا auto-sell.
