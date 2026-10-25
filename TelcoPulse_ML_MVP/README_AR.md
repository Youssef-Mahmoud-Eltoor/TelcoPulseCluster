# TelcoPulse ML (MVP) - توقع الحمل + توفير الطاقة + تصنيف السيلات

## بيعمل إيه
1. **توقع الحمل (24 ساعة قدام)** لكل سيل (`rb_utilization`) بموديل Gradient Boosting، ومقارنته دايماً بـ baseline "نفس الساعة الأسبوع اللي فات". لو الموديل مش أحسن، ما نعرضوش.
2. **تنبيه الازدحام**: أي ساعة متوقع فيها الاستخدام >= 80% (قابل للتعديل `CONGESTION_PCT`).
3. **كشف هدر الطاقة**: موديل بيحسب الطاقة المتوقعة من الحمل، والسيل اللي بيصرف أكتر بشكل ملحوظ من أقرانه بيتعلّم (`waste_flag`).
4. **تصنيف السيلات** (سكني / أعمال / ترفيهي-سياحي...) بـ K-Means على شكل الحمل اليومي.
تقويم مصر مدمج: الجمعة والسبت weekend، ورمضان وساعات الإفطار (عدّل `RAMADAN` كل سنة على التواريخ الرسمية).

## التشغيل
pip install -r requirements.txt
python src/telcopulse_ml.py --source demo                       # داتا تجريبية (synthetic) للتجربة فقط
python src/telcopulse_ml.py --source csv --csv hourly.csv       # أعمدة: cell,ts,technology,rb_utilization,active_users_dl,energy
set CH_USER=... & set CH_PASSWORD=...
python src/telcopulse_ml.py --source clickhouse --write-clickhouse   # من telecom_db.kpi_batch_v ويكتب جداول ml_*
قبل أول تشغيل على ClickHouse: نفّذ sql/05_ml_tables.sql

## ملاحظة مهمة
الأرقام اللي في out/ طالعة من **داتا تجريبية مصنّعة** (حقنّا فيها أنماط مصرية وسيلات بتهدر طاقة) عشان نتأكد إن الكود شغال. مش نتايج شبكة حقيقية.
النتايج الحقيقية بتطلع لما تشغّله على داتا kpi_batch (أو داتا الشركة).
