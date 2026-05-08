"""
backfill_dag.py
===============
DAG สำหรับดึงข้อมูลย้อนหลัง — trigger ด้วยมือ
ใช้เมื่อต้องการ historical data หรือ re-run หลัง pipeline fail

วิธีใช้:
  1. ไป Airflow UI → DAGs → backfill_historical
  2. กด Trigger DAG w/ config
  3. ใส่ config: {"start_date": "2024-01-01", "end_date": "2024-03-31"}
"""

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def generate_date_range(**context):
    """สร้าง list ของวันที่ที่ต้องการดึงข้อมูลย้อนหลัง"""
    conf = context.get("dag_run").conf or {}
    start = conf.get("start_date", "2024-01-01")
    end = conf.get("end_date", (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))

    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()

    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(str(current))
        current += timedelta(days=1)

    log.info(f"Backfill dates: {start} to {end} ({len(dates)} days)")
    context["ti"].xcom_push(key="dates", value=dates)
    return dates


def trigger_etl_for_dates(**context):
    """Trigger ETL DAG สำหรับแต่ละวันใน date range"""
    from airflow.api.client.local_client import Client

    dates = context["ti"].xcom_pull(key="dates") or []
    client = Client(None, None)

    triggered = []
    for d in dates:
        try:
            run_id = f"backfill_{d}_{datetime.now().strftime('%H%M%S')}"
            client.trigger_dag(
                dag_id="thailand_weather_etl",
                run_id=run_id,
                execution_date=datetime.strptime(d, "%Y-%m-%d"),
                conf={"backfill": True},
            )
            triggered.append(d)
            log.info(f"Triggered ETL for {d}")
        except Exception as e:
            log.error(f"Failed to trigger {d}: {e}")

    log.info(f"Triggered {len(triggered)}/{len(dates)} backfill runs")
    return triggered


with DAG(
    dag_id="backfill_historical",
    description="Trigger ETL pipeline ย้อนหลัง (manual trigger only)",
    schedule_interval=None,  # Manual trigger เท่านั้น
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "backfill", "manual"],
    doc_md="""
    ## Backfill Historical Weather Data

    ### วิธีใช้
    1. Trigger DAG with config:
    ```json
    {
      "start_date": "2024-01-01",
      "end_date": "2024-03-31"
    }
    ```
    2. DAG จะ trigger `thailand_weather_etl` สำหรับแต่ละวัน
    """,
) as dag:

    generate_dates = PythonOperator(
        task_id="generate_date_range",
        python_callable=generate_date_range,
    )

    trigger_runs = PythonOperator(
        task_id="trigger_etl_runs",
        python_callable=trigger_etl_for_dates,
    )

    generate_dates >> trigger_runs
