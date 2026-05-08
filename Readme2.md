# 🌦️ Thailand Weather Data Pipeline
### อธิบายกระบวนการตั้งแต่ต้นจนจบ

---

## ภาพรวม: เราทำอะไรในโปรเจ็คนี้?

โปรเจ็คนี้คือระบบ **Data Pipeline** ที่ทำงานอัตโนมัติทุกวัน โดยมีหน้าที่หลักคือ:

> "ดึงข้อมูลสภาพอากาศของ 77 จังหวัดในประเทศไทย → ทำความสะอาดข้อมูล → เก็บลง Database → แสดงผลบน Dashboard"

ทั้งหมดนี้ไม่ต้องมีคนมานั่งรันเองทุกวัน เพราะ **Apache Airflow** จะจัดการให้อัตโนมัติ

---

## ข้อมูลต้นทางมาจากไหน?

### แหล่งข้อมูล: Open-Meteo API
- **Website:** https://open-meteo.com
- **ฟรี 100%** ไม่ต้องสมัคร ไม่ต้อง API Key
- เป็น API ที่รวบรวมข้อมูลจากโมเดลพยากรณ์อากาศระดับโลก ได้แก่:
  - **ECMWF** (European Centre for Medium-Range Weather Forecasts) — จากยุโรป แม่นที่สุด
  - **GFS** (Global Forecast System) — จาก NASA/NOAA อเมริกา
  - **DWD** — จากเยอรมนี

### ข้อมูลต้นทางจริงๆ มาจากไหน?
```
สถานีวัดอากาศจริงทั่วโลก (ภาคพื้นดิน + ดาวเทียม)
              ↓
    โมเดลคำนวณพยากรณ์ (ECMWF, GFS, DWD)
              ↓
    Open-Meteo รวบรวมและให้บริการผ่าน API ฟรี
              ↓
         Pipeline ของเรา
```

### เราขอข้อมูลอะไรจาก API?
เราส่ง **lat/lon ของแต่ละจังหวัด** ไปถาม แล้ว API จะตอบกลับมาเป็น JSON รายชั่วโมง:

```
# ตัวอย่าง URL ที่เราเรียก (กรุงเทพฯ)
https://api.open-meteo.com/v1/forecast
  ?latitude=13.7563
  &longitude=100.5018
  &hourly=temperature_2m,relative_humidity_2m,precipitation,
          wind_speed_10m,wind_direction_10m,weather_code,
          surface_pressure,cloud_cover
  &start_date=2026-05-03
  &end_date=2026-05-03
  &timezone=Asia/Bangkok
```

API จะตอบกลับมาแบบนี้:
```json
{
  "hourly": {
    "time":           ["2026-05-03T00:00", "2026-05-03T01:00", ...],
    "temperature_2m": [28.5, 27.9, 27.3, 28.1, ...],
    "precipitation":  [0.0,  0.0,  1.2,  0.5,  ...],
    "wind_speed_10m": [12.3, 10.1, 8.5,  9.2,  ...]
  }
}
```

ได้ **24 rows ต่อจังหวัด** × 77 จังหวัด = **1,848 records ต่อวัน**

---

## Tools ที่ใช้มีอะไรบ้าง?

| Tool | หน้าที่ | ทำไมถึงเลือก |
|------|---------|-------------|
| **Apache Airflow** | จัดคิวและรัน pipeline อัตโนมัติทุกวัน | มาตรฐานอุตสาหกรรม Data Engineering |
| **Python requests** | ดึงข้อมูลจาก Open-Meteo API | library ยอดนิยมสำหรับ HTTP request |
| **PostgreSQL** | เก็บข้อมูลแบบ 3 layers | รองรับ JSONB + มี index ที่ดี |
| **dbt** | transform ข้อมูลด้วย SQL | ทำ data lineage + test ได้ |
| **Grafana** | แสดงผล dashboard | เชื่อมต่อ PostgreSQL ได้โดยตรง |
| **Docker** | รันทุกอย่างบนเครื่องเดียว | ไม่ต้องติดตั้งอะไรเพิ่ม |

---

## กระบวนการทำงานตั้งแต่ต้นจนจบ

### ภาพรวม Flow
```
[Open-Meteo API]
      │  HTTP GET (ทุกวัน 06:00 น.)
      ↓
[Task 1: Extract]  ← Python requests.get()
      │  JSON ดิบ 77 จังหวัด
      ↓
[Task 2: Validate] ← ตรวจสอบ null / ค่าผิดปกติ
      │  ผ่านเกณฑ์ > 70%
      ↓
[Task 3: Transform] ← แปลง JSON → typed columns
      │  1,848 rows
      ↓
[Task 4: Load Mart] ← Aggregate เป็น daily summary
      │  77 rows (1 จังหวัด / 1 วัน)
      ↓
[Task 5: Notify]   ← Log สรุปผล
      │
      ↓
[Grafana Dashboard] ← Query จาก PostgreSQL
```

---

### Task 1: Extract — ดึงข้อมูลจาก API

**ทำอะไร:**
- วนลูป 77 จังหวัด ทีละจังหวัด
- ส่ง HTTP GET ไปที่ Open-Meteo API พร้อม lat/lon
- รับ JSON กลับมาเก็บลง `raw.weather_responses` ทันที ยังไม่แตะเนื้อหา

**เก็บลง Table:**
```
raw.weather_responses
├── province_code  = "10"
├── province_name  = "กรุงเทพฯ"
├── response_json  = { ...JSON ดิบทั้งก้อน... }
├── data_date      = 2026-05-03
└── fetched_at     = เวลาที่ดึง
```

**ทำไมเก็บ JSON ดิบไว้ก่อน?**
เพราะถ้า step ถัดไปมีบัก เรายังมีข้อมูลดิบให้ย้อนกลับมาแปลงใหม่ได้ ไม่ต้องดึง API ซ้ำ

---

### Task 2: Validate — ตรวจสอบคุณภาพข้อมูล

**ทำอะไร:**
- เช็คว่า JSON ที่ได้มามี structure ถูกต้องไหม
- เช็คว่าอุณหภูมิอยู่ในช่วง 5–45°C (สมเหตุสมผลสำหรับไทย)
- เช็คว่า temperature array ไม่ว่างเปล่า
- ถ้ามีจังหวัดที่ fail เกิน 30% → หยุด pipeline ทันที ไม่ให้ข้อมูลผิดๆ ไหลเข้า DB

**ทำไมต้องมี Validate?**
เพราะ API อาจมีปัญหาชั่วคราว หรือส่งค่าแปลกๆ กลับมา ถ้าไม่เช็คก่อน ข้อมูลเสียจะเข้าไปอยู่ใน dashboard โดยไม่รู้ตัว

---

### Task 3: Transform — แปลงข้อมูล

**ทำอะไร:**
- เปิด JSON ดิบแต่ละจังหวัด
- "Flatten" array รายชั่วโมงออกมาเป็น rows ทีละแถว
- แปลง type ให้ถูกต้อง (string → decimal, integer ฯลฯ)
- จัดการค่า null

**ก่อน Transform (JSON ดิบ):**
```json
{
  "time":           ["00:00", "01:00", "02:00"],
  "temperature_2m": [28.5,    27.9,    27.3  ],
  "precipitation":  [0.0,     0.0,     1.2   ]
}
```

**หลัง Transform (rows ใน staging):**
```
province  | measured_at          | temperature | precipitation
กรุงเทพฯ  | 2026-05-03 00:00:00  | 28.5        | 0.0
กรุงเทพฯ  | 2026-05-03 01:00:00  | 27.9        | 0.0
กรุงเทพฯ  | 2026-05-03 02:00:00  | 27.3        | 1.2
```

เก็บลง `staging.weather_hourly`

---

### Task 4: Load Mart — สรุปข้อมูลรายวัน

**ทำอะไร:**
- เอา 24 rows (รายชั่วโมง) ของแต่ละจังหวัด มา Aggregate เป็น 1 row รายวัน
- คำนวณ avg / max / min / sum

**ตัวอย่าง:**
```
24 rows ของกรุงเทพฯ วันที่ 3 พ.ค.
    ↓ AVG, MAX, MIN, SUM
1 row สรุป:
  temp_avg = 29.5°C
  temp_max = 35.2°C
  temp_min = 26.1°C
  total_rain = 12.5mm
```

เก็บลง `mart.daily_summary` → Grafana ดึงจาก table นี้โดยตรง

---

### Task 5: Notify — สรุปผล

- Log ชื่อ 5 จังหวัดร้อนสุดของวัน
- บันทึกสถิติ pipeline ลง `mart.pipeline_run_log`
- ต่อยอดได้: ส่ง LINE Notify / Slack / Email

---

## Database: 3 Layers (Medallion Architecture)

```
raw layer       → เก็บข้อมูลดิบ ห้ามแก้ไข (Bronze)
staging layer   → ข้อมูล clean + typed       (Silver)
mart layer      → ข้อมูล aggregated พร้อมใช้  (Gold)
```

ทำแบบนี้เพราะถ้าเกิด bug ใน transform เราสามารถ re-run จาก raw ได้เลย โดยไม่ต้องดึง API ใหม่

---

## Apache Airflow ทำหน้าที่อะไร?

Airflow คือ **ตัวจัดการ pipeline** ครับ เปรียบได้กับ "นาฬิกาปลุก + ผู้จัดการ" ที่:

1. **ตื่นขึ้นมาทุกวัน 06:00 น.** ตามที่ตั้งไว้
2. **สั่งรัน task ตามลำดับ** extract → validate → transform → load → notify
3. **ถ้า task ไหน fail** → retry อัตโนมัติ 3 ครั้ง
4. **เก็บ log ทุก run** ดูย้อนหลังได้ว่าวันไหนสำเร็จ/ล้มเหลว
5. **ถ้า task ก่อนหน้า fail** → task ถัดไปจะไม่รัน (ป้องกันข้อมูลเสียเข้า DB)

---

## Grafana ดึงข้อมูลยังไง?

Grafana เชื่อมต่อกับ PostgreSQL โดยตรง แล้วใช้ SQL query ดึงข้อมูลมาแสดงผล เช่น:

```sql
-- กราฟอุณหภูมิรายวัน
SELECT data_date, AVG(temp_avg), MAX(temp_max)
FROM mart.daily_summary
GROUP BY data_date
ORDER BY data_date
```

Dashboard refresh ทุก 1 ชั่วโมง พอ pipeline รันเสร็จ ข้อมูลจะอัปเดตเองอัตโนมัติ

---

## สรุปทั้งหมดในภาพเดียว

```
Open-Meteo API (ข้อมูลอากาศฟรี)
    │
    │  HTTP GET request (Python requests)
    │  ← lat/lon ของ 77 จังหวัด
    │  → JSON 24 ชั่วโมง / จังหวัด
    ↓
Apache Airflow (รันอัตโนมัติทุกวัน 06:00 น.)
    │
    ├── Task 1: Extract   → raw.weather_responses    (JSON ดิบ)
    ├── Task 2: Validate  → เช็คคุณภาพ ก่อนผ่าน
    ├── Task 3: Transform → staging.weather_hourly   (1,848 rows)
    ├── Task 4: Load Mart → mart.daily_summary        (77 rows)
    └── Task 5: Notify    → log สรุปผล
    │
    ↓
PostgreSQL Database
    │
    ↓
Grafana Dashboard
    → กราฟอุณหภูมิรายวัน
    → 10 จังหวัดร้อนสุด
    → อุณหภูมิแต่ละภาค
    → Pipeline run log
```