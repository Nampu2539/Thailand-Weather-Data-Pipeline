# 🌦️ Thailand Weather Data Pipeline

ระบบ Data Pipeline ที่ดึงข้อมูลสภาพอากาศ **77 จังหวัดของไทย** จาก Open-Meteo API ทุกวัน
ผ่าน Apache Airflow → PostgreSQL DWH → Grafana Dashboard

---

## 🏗️ Architecture

```
Open-Meteo API (ฟรี)
       │
       ▼
┌─────────────────────────────────────────────┐
│          Apache Airflow (port 8080)          │
│                                             │
│  extract → validate → transform → load_mart │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│         PostgreSQL — Weather DWH             │
│                                              │
│  raw.weather_responses    (JSON ดิบ)         │
│  staging.weather_hourly   (typed + cleaned)  │
│  mart.daily_summary       (aggregated)       │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
         Grafana (port 3000)
         ─ Temperature map
         ─ Rain time series
         ─ Pipeline log table
```

---

## 🚀 Quick Start (5 นาที)

### สิ่งที่ต้องมี
- Docker Desktop (macOS/Windows) หรือ Docker Engine (Linux)
- RAM อย่างน้อย 4GB สำหรับ Airflow

### Step 1: Clone และ setup

```bash
# Clone project
git clone <your-repo-url>
cd weather-pipeline

# สร้าง .env file
echo "AIRFLOW_UID=$(id -u)" > .env
```

### Step 2: Start services

```bash
make up
# หรือ: docker compose up -d
```

รอประมาณ 2-3 นาทีให้ทุกอย่าง initialize เสร็จ

### Step 3: เข้า Airflow UI

เปิด browser → **http://localhost:8080**

```
Username: admin
Password: admin
```

### Step 4: Trigger DAG ครั้งแรก

```bash
make trigger-dag
# หรือคลิก ▶ (Trigger DAG) ใน Airflow UI
```

DAG จะรัน 5 tasks:
1. `extract` — ดึงข้อมูลจาก Open-Meteo API (ใช้เวลา ~2 นาที)
2. `validate` — ตรวจสอบ data quality
3. `transform` — แปลง JSON → typed rows
4. `load_mart` — สร้าง daily summary
5. `notify` — log สรุปผล

### Step 5: ดู Dashboard

เปิด browser → **http://localhost:3000**

```
Username: admin
Password: admin
```

ไปที่ Dashboards → Weather → **Thailand Weather Dashboard**

---

## 📁 Project Structure

```
weather-pipeline/
├── dags/
│   ├── thailand_weather_etl.py   ← DAG หลัก (รันทุกวัน)
│   └── backfill_dag.py           ← ดึงข้อมูลย้อนหลัง
├── config/
│   └── provinces.py              ← รายชื่อ 77 จังหวัด + lat/lon
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_weather_hourly.sql
│       └── marts/
│           └── mart_monthly_province.sql
├── grafana/
│   ├── datasources.yml
│   └── dashboards/
│       └── thailand_weather.json
├── scripts/
│   └── init_db.sh                ← สร้าง schema อัตโนมัติ
├── docker-compose.yml
├── requirements.txt
├── Makefile
└── README.md
```

---

## 🗄️ Database Schema

### raw.weather_responses
| Column | Type | Description |
|--------|------|-------------|
| province_code | VARCHAR | รหัสจังหวัด |
| province_name | VARCHAR | ชื่อจังหวัด |
| response_json | JSONB | JSON ดิบจาก API |
| data_date | DATE | วันที่ของข้อมูล |
| fetched_at | TIMESTAMPTZ | เวลาที่ดึงข้อมูล |

### staging.weather_hourly
| Column | Type | Description |
|--------|------|-------------|
| province_code | VARCHAR | รหัสจังหวัด |
| measured_at | TIMESTAMPTZ | เวลาที่วัด (รายชั่วโมง) |
| temperature_2m | DECIMAL | อุณหภูมิ 2m (°C) |
| humidity | INTEGER | ความชื้นสัมพัทธ์ (%) |
| precipitation | DECIMAL | ปริมาณฝน (mm) |
| wind_speed | DECIMAL | ความเร็วลม (km/h) |
| weather_code | INTEGER | WMO weather code |

### mart.daily_summary
| Column | Type | Description |
|--------|------|-------------|
| province_code | VARCHAR | รหัสจังหวัด |
| region | VARCHAR | ภาค (North/Central/Northeast/South) |
| data_date | DATE | วันที่ |
| temp_avg/max/min | DECIMAL | อุณหภูมิเฉลี่ย/สูงสุด/ต่ำสุด (°C) |
| total_precipitation | DECIMAL | ปริมาณฝนสะสม (mm) |
| data_quality_pct | DECIMAL | % ข้อมูลที่ผ่าน validation |

---

## 🛠️ คำสั่งที่ใช้บ่อย

```bash
# ดู logs ของ scheduler
make logs

# Trigger DAG ด้วยมือ
make trigger-dag

# ดึงข้อมูลย้อนหลัง 7 วัน
make backfill

# Query ข้อมูล: 10 จังหวัดร้อนสุดวันนี้
make query-hot

# ดู pipeline run history
make query-log

# Run dbt models
make dbt-run

# Stop ทุกอย่าง
make down

# Clear ทุกอย่าง (รวม database)
make clean
```

---

## 🔧 Configuration

### เปลี่ยน Schedule
แก้ใน `dags/thailand_weather_etl.py`:
```python
schedule_interval="0 23 * * *",  # 06:00 ICT ทุกวัน
# เปลี่ยนเป็น hourly: "0 * * * *"
# เปลี่ยนเป็น weekly: "0 23 * * 0"
```

### เปิด Backfill (ดึงย้อนหลัง)
```python
catchup=True,   # เปลี่ยนจาก False เป็น True
start_date=datetime(2024, 1, 1),
```

### เพิ่ม Email Alert เมื่อ DAG fail
```python
default_args = {
    "email": ["your-email@example.com"],
    "email_on_failure": True,
}
```

---

## 📊 ต่อยอดได้เพิ่มเติม

| Feature | วิธีทำ |
|---------|--------|
| Alert น้ำท่วม | เพิ่ม task ตรวจ `total_precipitation > 50mm` แล้วส่ง LINE Notify |
| PM2.5 data | เพิ่ม DAG ดึงจาก IQAir API มา join กัน |
| ML forecast | Train sklearn model ทำนายอากาศ 3 วัน บน mart data |
| dbt tests | เพิ่ม `schema.yml` ตรวจสอบ null, range, unique |
| Slack alerts | ใช้ Airflow `SlackWebhookOperator` |
| S3/GCS backup | เพิ่ม task อัพโหลด raw JSON ไป cloud storage |

---

## 🐛 Troubleshooting

**Airflow ไม่ขึ้น:**
```bash
docker compose logs airflow-init
# มักเกิดจาก RAM ไม่พอ หรือ port 8080 ถูกใช้งานอยู่
```

**DAG ไม่ปรากฏใน UI:**
```bash
docker compose exec airflow-scheduler airflow dags list
# ตรวจสอบ syntax error ใน DAG file
docker compose exec airflow-scheduler python /opt/airflow/dags/thailand_weather_etl.py
```

**Database connection error:**
```bash
docker compose exec postgres psql -U weather -d weather_db -c "\dt raw.*"
# ตรวจสอบว่า schema ถูกสร้างแล้ว
```

---

## 📡 Data Source

- **Open-Meteo API**: https://open-meteo.com/
- ฟรี ไม่ต้อง API key
- ข้อมูลสภาพอากาศรายชั่วโมง 7 วันล่วงหน้า + ย้อนหลัง
- Rate limit: 10,000 req/day (พอสำหรับ 77 จังหวัด/วัน)
