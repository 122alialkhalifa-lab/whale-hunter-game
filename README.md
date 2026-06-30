# Whale Hunter Radar — 10-Min Leverage Sprint Scientists V12

لعبة مراقبة حيتان بأسلوب Game HUD على الآيباد. هذه النسخة تجعل كل Bot يدخل سباقًا وهميًا مدته 10 دقائق: هدفه أعلى ربح وهمي ممكن في أقل وقت ممكن، مع إمكانية استخدام رافعة وهمية داخل اللعبة فقط.

## قواعد V12

- 500 Bot، كل واحد يبدأ بـ $1000 وهمية.
- مدة الجولة الافتراضية: 600 ثانية = 10 دقائق.
- الرافعة وهمية فقط، افتراضيًا حتى 20x ويمكن تغييرها من `MAX_FAKE_LEVERAGE`.
- لا يوجد تداول حقيقي، لا Binance API keys، لا أوامر شراء أو بيع.
- الأسعار والمؤشرات تأتي من Binance Futures public WebSocket.
- كل Bot يحسب معادلة سرعة الربح:

```text
V = PnL%/min + Lev + sign(direction)·Δprice
Δwᵢ = μ·V·xᵢ
```

إذا الربح يتحقق بسرعة، يرفع وزن المؤشرات التي ساعدته. إذا انعكس السعر أو حصلت تصفية وهمية، يخفض وزنها وينشر تقريرًا لباقي الروبوتات.

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
rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/
npm start
```

افتح:

```text
http://localhost:3000
```

## على Render

Build Command:

```bash
rm -f package-lock.json && rm -f package-lock.json && npm install --registry=https://registry.npmjs.org/ --registry=https://registry.npmjs.org/
```

Start Command:

```bash
node server.mjs
```

يفضل Region: Singapore.

## ملاحظات أمان

مراقبة ومحاكاة فقط. ليست توصية مالية. الرافعة قد تصفّر الحساب.
لا توجد مفاتيح Binance، لا تنفيذ صفقات، لا auto-buy، لا auto-sell.


## V11 Living Brain Upgrade

هذه النسخة لا تنتظر نهاية الجولة فقط. كل 1.5 ثانية تقريبًا يرسل بعض الروبوتات نبض تفكير حي:

- فرضية الروبوت الحالية.
- معادلته اللحظية `L = sign(dir)·Δprice + PnL/8`.
- تحديث أوزان صغير `live Δwᵢ = μ·L·xᵢ`.
- هل الفرضية تتأكد أو تبدأ تخدع الروبوت.

الصفحة مجرد شاشة مشاهدة؛ التحليل والتعلم يعملان على السيرفر ما دام Render صاحي.
