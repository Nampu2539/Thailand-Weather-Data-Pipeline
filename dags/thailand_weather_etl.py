"""
thailand_weather_etl.py
========================
DAG หลักสำหรับดึงข้อมูลสภาพอากาศ 77 จังหวัดของไทย
รันทุกวัน เวลา 06:00 น. ICT (23:00 UTC วันก่อน)

Flow: extract >> validate >> transform >> load >> dbt_run >> notify
"""

import json
import time
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Any

import requests
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import sys
sys.path.insert(0, '/opt/airflow/config')
from provinces import PROVINCES, WMO_CODES

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Default Args
# ─────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=3),
    "email_on_failure": False,  # เปลี่ยนเป็น True และใส่ email หากต้องการ
    "email_on_retry": False,
}

# ─────────────────────────────────────────
# Helper: Database Connection
# ─────────────────────────────────────────
def get_db_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="weather_db",
        user="weather",
        password="weather"
    )


# ─────────────────────────────────────────
# TASK 1: Extract — ดึงข้อมูลจาก Open-Meteo API
# ─────────────────────────────────────────
def extract_weather(**context):
    """
    ดึงข้อมูลสภาพอากาศจาก Open-Meteo API สำหรับ 77 จังหวัด
    เก็บผลลัพธ์เป็น JSON ดิบใน raw.weather_responses
    """
    data_date = context["ds"]  # YYYY-MM-DD ของ DAG run
    dag_run_id = context["run_id"]
    start_time = time.time()

    log.info(f"Starting extraction for date: {data_date}")

    # Open-Meteo API endpoint (ฟรี ไม่ต้อง API key)
    API_URL = "https://api.open-meteo.com/v1/forecast"

    # Variables ที่ดึงจาก API
    HOURLY_VARS = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "wind_direction_10m",
        "weather_code",
        "surface_pressure",
        "cloud_cover",
    ]

    results = []
    failed = []

    conn = get_db_conn()
    cursor = conn.cursor()

    # ลบข้อมูลเก่าของวันนี้ก่อน (idempotent)
    cursor.execute(
        "DELETE FROM raw.weather_responses WHERE data_date = %s",
        (data_date,)
    )

    for province in PROVINCES:
        try:
            params = {
                "latitude": province["lat"],
                "longitude": province["lon"],
                "hourly": ",".join(HOURLY_VARS),
                "start_date": data_date,
                "end_date": data_date,
                "timezone": "Asia/Bangkok",
                "wind_speed_unit": "kmh",
            }

            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Insert เข้า raw layer
            cursor.execute("""
                INSERT INTO raw.weather_responses
                    (province_code, province_name, latitude, longitude,
                     response_json, data_date, dag_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                province["code"],
                province["name"],
                province["lat"],
                province["lon"],
                json.dumps(data),
                data_date,
                dag_run_id
            ))

            results.append(province["code"])
            log.info(f"✓ Extracted: {province['name_en']}")

            # Rate limiting — ป้องกัน API throttle
            time.sleep(0.3)

        except Exception as e:
            failed.append({"province": province["name_en"], "error": str(e)})
            log.error(f"✗ Failed: {province['name_en']} — {e}")

    conn.commit()
    cursor.close()
    conn.close()

    duration = time.time() - start_time
    log.info(f"Extraction done: {len(results)} OK, {len(failed)} failed in {duration:.1f}s")

    if len(failed) > len(PROVINCES) * 0.2:  # มากกว่า 20% fail → error
        raise ValueError(f"Too many failures: {failed}")

    # ส่งต่อผ่าน XCom
    context["ti"].xcom_push(key="extract_summary", value={
        "ok": len(results),
        "failed": len(failed),
        "duration": duration,
        "failed_provinces": failed,
    })

    return len(results)


# ─────────────────────────────────────────
# TASK 2: Validate — ตรวจสอบ Data Quality
# ─────────────────────────────────────────
def validate_data(**context):
    """
    ตรวจสอบข้อมูลดิบจาก raw layer
    - จำนวน records ครบ?
    - ค่า temperature อยู่ในช่วงที่สมเหตุสมผล?
    - ไม่มี null เกินเกณฑ์?
    """
    data_date = context["ds"]
    conn = get_db_conn()
    cursor = conn.cursor()

    # 1. ตรวจสอบจำนวน provinces
    cursor.execute(
        "SELECT COUNT(*) FROM raw.weather_responses WHERE data_date = %s",
        (data_date,)
    )
    total_provinces = cursor.fetchone()[0]

    if total_provinces == 0:
        raise ValueError(f"No data found for {data_date}!")

    log.info(f"Found {total_provinces}/{len(PROVINCES)} provinces")

    # 2. ตรวจสอบ JSON structure ของแต่ละ province
    cursor.execute("""
        SELECT province_code, province_name, response_json
        FROM raw.weather_responses
        WHERE data_date = %s
    """, (data_date,))

    issues = []
    valid_count = 0

    for row in cursor.fetchall():
        code, name, resp = row
        hourly = resp.get("hourly", {})
        temps = hourly.get("temperature_2m", [])

        if not temps:
            issues.append(f"{name}: empty temperature array")
            continue

        valid_temps = [t for t in temps if t is not None]
        if not valid_temps:
            issues.append(f"{name}: all temperatures null")
            continue

        # ตรวจสอบ range ที่สมเหตุสมผลสำหรับประเทศไทย (5°C - 45°C)
        min_temp = min(valid_temps)
        max_temp = max(valid_temps)
        if min_temp < 5 or max_temp > 45:
            issues.append(f"{name}: suspicious temperature range {min_temp}–{max_temp}°C")

        valid_count += 1

    cursor.close()
    conn.close()

    quality_pct = (valid_count / total_provinces * 100) if total_provinces > 0 else 0
    log.info(f"Validation: {valid_count}/{total_provinces} valid ({quality_pct:.1f}%)")

    if issues:
        log.warning(f"Data quality issues: {issues}")

    if quality_pct < 70:
        raise ValueError(f"Data quality too low: {quality_pct:.1f}%")

    context["ti"].xcom_push(key="validation_summary", value={
        "total": total_provinces,
        "valid": valid_count,
        "quality_pct": quality_pct,
        "issues": issues,
    })

    return quality_pct


# ─────────────────────────────────────────
# TASK 3: Transform — แปลง JSON → Typed rows
# ─────────────────────────────────────────
def transform_data(**context):
    """
    แปลงข้อมูลจาก raw.weather_responses (JSON)
    เป็น typed rows ใน staging.weather_hourly
    - Flatten JSON hourly arrays
    - Type casting
    - Null handling
    """
    data_date = context["ds"]
    conn = get_db_conn()
    cursor = conn.cursor()

    # ลบ staging ของวันนี้ก่อน (idempotent)
    cursor.execute(
        "DELETE FROM staging.weather_hourly WHERE data_date = %s",
        (data_date,)
    )

    cursor.execute("""
        SELECT province_code, province_name, latitude, longitude, response_json
        FROM raw.weather_responses
        WHERE data_date = %s
    """, (data_date,))

    rows_to_insert = []
    total_hours = 0

    for province_code, province_name, lat, lon, resp in cursor.fetchall():
        province_info = next(
            (p for p in PROVINCES if p["code"] == province_code), None
        )
        region = province_info["region"] if province_info else "Unknown"

        hourly = resp.get("hourly", {})
        times = hourly.get("time", [])

        for i, ts_str in enumerate(times):
            try:
                measured_at = datetime.fromisoformat(ts_str)

                def safe_get(key, idx):
                    arr = hourly.get(key, [])
                    if idx < len(arr) and arr[idx] is not None:
                        return arr[idx]
                    return None

                temp = safe_get("temperature_2m", i)
                humidity = safe_get("relative_humidity_2m", i)
                precip = safe_get("precipitation", i)
                wind_speed = safe_get("wind_speed_10m", i)
                wind_dir = safe_get("wind_direction_10m", i)
                weather_code = safe_get("weather_code", i)
                pressure = safe_get("surface_pressure", i)
                cloud = safe_get("cloud_cover", i)

                # Validity check
                is_valid = temp is not None and 5 <= temp <= 45

                rows_to_insert.append((
                    province_code, province_name, lat, lon,
                    measured_at, data_date,
                    temp, humidity, precip, wind_speed, wind_dir,
                    weather_code, pressure, cloud, is_valid
                ))
                total_hours += 1

            except Exception as e:
                log.warning(f"Skipping {province_name} hour {i}: {e}")

    # Batch insert สำหรับ performance
    if rows_to_insert:
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging.weather_hourly (
                province_code, province_name, latitude, longitude,
                measured_at, data_date,
                temperature_2m, humidity, precipitation, wind_speed, wind_direction,
                weather_code, surface_pressure, cloud_cover, is_valid
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (province_code, measured_at) DO UPDATE SET
                temperature_2m = EXCLUDED.temperature_2m,
                humidity = EXCLUDED.humidity,
                precipitation = EXCLUDED.precipitation,
                loaded_at = NOW()
        """, rows_to_insert, page_size=500)

    conn.commit()
    cursor.close()
    conn.close()

    log.info(f"Transformed {total_hours} hourly records for {data_date}")
    context["ti"].xcom_push(key="transform_summary", value={"total_rows": total_hours})
    return total_hours


# ─────────────────────────────────────────
# TASK 4: Load — สร้าง Daily Summary ใน Mart
# ─────────────────────────────────────────
def load_to_mart(**context):
    """
    Aggregate hourly data → daily summary
    เก็บใน mart.daily_summary สำหรับ Grafana dashboard
    """
    data_date = context["ds"]
    dag_run_id = context["run_id"]
    conn = get_db_conn()
    cursor = conn.cursor()

    # ลบ mart ของวันนี้ก่อน (idempotent)
    cursor.execute("DELETE FROM mart.daily_summary WHERE data_date = %s", (data_date,))

    # Aggregate query
    cursor.execute("""
        INSERT INTO mart.daily_summary (
            province_code, province_name, latitude, longitude, region, data_date,
            temp_avg, temp_max, temp_min,
            humidity_avg, total_precipitation,
            wind_speed_avg, wind_speed_max,
            dominant_weather, weather_description,
            cloud_cover_avg, hours_recorded, data_quality_pct
        )
        WITH province_region AS (
            SELECT DISTINCT
                w.province_code,
                CASE
                    WHEN w.province_code IN ('50','51','52','53','54','55','56','57','58',
                                             '60','61','62','63','64','65','66','67') THEN 'North'
                    WHEN w.province_code IN ('80','81','82','83','84','85','86',
                                             '90','91','92','93','94','95','96') THEN 'South'
                    WHEN w.province_code IN ('30','31','32','33','34','35','36','37','38',
                                             '39','40','41','42','43','44','45','46','47','48','49') THEN 'Northeast'
                    ELSE 'Central'
                END AS region
            FROM staging.weather_hourly w
        )
        SELECT
            h.province_code,
            h.province_name,
            h.latitude,
            h.longitude,
            pr.region,
            h.data_date,
            ROUND(AVG(h.temperature_2m)::numeric, 2),
            ROUND(MAX(h.temperature_2m)::numeric, 2),
            ROUND(MIN(h.temperature_2m)::numeric, 2),
            ROUND(AVG(h.humidity)::numeric, 2),
            ROUND(SUM(COALESCE(h.precipitation, 0))::numeric, 2),
            ROUND(AVG(h.wind_speed)::numeric, 2),
            ROUND(MAX(h.wind_speed)::numeric, 2),
            MODE() WITHIN GROUP (ORDER BY h.weather_code),
            NULL,  -- weather_description จะ update ทีหลัง
            ROUND(AVG(h.cloud_cover)::numeric, 2),
            COUNT(*),
            ROUND((SUM(CASE WHEN h.is_valid THEN 1 ELSE 0 END)::decimal / COUNT(*) * 100)::numeric, 2)
        FROM staging.weather_hourly h
        JOIN province_region pr ON h.province_code = pr.province_code
        WHERE h.data_date = %s
        GROUP BY h.province_code, h.province_name, h.latitude, h.longitude, pr.region, h.data_date
        ON CONFLICT (province_code, data_date) DO UPDATE SET
            temp_avg = EXCLUDED.temp_avg,
            temp_max = EXCLUDED.temp_max,
            temp_min = EXCLUDED.temp_min,
            total_precipitation = EXCLUDED.total_precipitation,
            updated_at = NOW()
    """, (data_date,))

    # Update weather description จาก WMO code
    for code, desc in WMO_CODES.items():
        cursor.execute("""
            UPDATE mart.daily_summary
            SET weather_description = %s
            WHERE data_date = %s AND dominant_weather = %s
        """, (desc, data_date, code))

    # Log pipeline run
    transform_summary = context["ti"].xcom_pull(key="transform_summary") or {}
    cursor.execute("""
        INSERT INTO mart.pipeline_run_log
            (dag_run_id, data_date, step, provinces_total, provinces_ok,
             records_inserted, status)
        SELECT %s, %s, 'full_etl', %s, COUNT(*), %s, 'success'
        FROM mart.daily_summary WHERE data_date = %s
    """, (dag_run_id, data_date, len(PROVINCES),
          transform_summary.get("total_rows", 0), data_date))

    conn.commit()

    # ดึงสรุปผล
    cursor.execute("""
        SELECT COUNT(*), ROUND(AVG(temp_avg)::numeric, 2), MAX(total_precipitation)
        FROM mart.daily_summary WHERE data_date = %s
    """, (data_date,))
    count, avg_temp, max_rain = cursor.fetchone()

    cursor.close()
    conn.close()

    log.info(f"Mart loaded: {count} provinces, avg temp {avg_temp}°C, max rain {max_rain}mm")
    return {"provinces": count, "avg_temp": avg_temp, "max_rain": max_rain}


# ─────────────────────────────────────────
# TASK 5: Notify — สรุปผลลัพธ์
# ─────────────────────────────────────────
def notify_success(**context):
    """สรุปผลใน Airflow logs (ต่อยอด: ส่ง Slack/Line/Email ได้)"""
    data_date = context["ds"]
    conn = get_db_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            province_name, temp_avg, temp_max, temp_min,
            total_precipitation, weather_description
        FROM mart.daily_summary
        WHERE data_date = %s
        ORDER BY temp_avg DESC
        LIMIT 5
    """, (data_date,))

    top5 = cursor.fetchall()
    cursor.close()
    conn.close()

    log.info("=" * 60)
    log.info(f"  Weather Pipeline Success — {data_date}")
    log.info("=" * 60)
    log.info("  Top 5 Hottest Provinces:")
    for row in top5:
        name, avg, mx, mn, rain, desc = row
        log.info(f"  {name:20s} avg:{avg}°C  max:{mx}°C  rain:{rain}mm  [{desc}]")
    log.info("=" * 60)


# ─────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────
with DAG(
    dag_id="thailand_weather_etl",
    description="ดึงข้อมูลสภาพอากาศ 77 จังหวัด → PostgreSQL DWH → Grafana",
    schedule_interval="0 23 * * *",  # 06:00 ICT = 23:00 UTC วันก่อน
    start_date=datetime(2024, 1, 1),
    catchup=False,  # เปลี่ยนเป็น True เพื่อดึงย้อนหลัง
    default_args=default_args,
    tags=["weather", "thailand", "etl", "daily"],
    max_active_runs=1,
    doc_md="""
    ## Thailand Weather ETL Pipeline
    ดึงข้อมูลสภาพอากาศ 77 จังหวัดจาก [Open-Meteo](https://open-meteo.com/) ทุกวัน

    ### Flow
    `extract` → `validate` → `transform` → `load_mart` → `notify`

    ### Data Layers
    - **raw**: JSON ดิบจาก API
    - **staging**: typed + cleaned hourly records
    - **mart**: daily aggregates สำหรับ dashboard
    """,
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_weather,
        doc="ดึงข้อมูลจาก Open-Meteo API → raw.weather_responses",
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data,
        doc="ตรวจสอบ data quality ก่อน transform",
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        doc="Flatten JSON → staging.weather_hourly",
    )

    t_load = PythonOperator(
        task_id="load_mart",
        python_callable=load_to_mart,
        doc="Aggregate daily summary → mart.daily_summary",
    )

    t_notify = PythonOperator(
        task_id="notify",
        python_callable=notify_success,
        doc="Log สรุปผลลัพธ์",
    )

    # ─── Task Dependencies ───
    t_extract >> t_validate >> t_transform >> t_load >> t_notify
