.PHONY: up down restart logs status trigger-dag backfill clean

# ─── Start all services ───
up:
	chmod +x scripts/init_db.sh
	docker compose up -d
	@echo ""
	@echo "  ✓ Starting services..."
	@echo "  ┌─────────────────────────────────────────┐"
	@echo "  │  Airflow UI  → http://localhost:8080     │"
	@echo "  │  Grafana     → http://localhost:3000     │"
	@echo "  │  Username: admin  Password: admin        │"
	@echo "  └─────────────────────────────────────────┘"

# ─── Stop all services ───
down:
	docker compose down

# ─── Restart ───
restart:
	docker compose restart

# ─── View logs ───
logs:
	docker compose logs -f airflow-scheduler

# ─── Show status ───
status:
	docker compose ps

# ─── Trigger DAG manually (today) ───
trigger-dag:
	docker compose exec airflow-scheduler airflow dags trigger thailand_weather_etl

# ─── Trigger with specific date ───
trigger-date:
	@read -p "Enter date (YYYY-MM-DD): " DATE; \
	docker compose exec airflow-scheduler airflow dags trigger thailand_weather_etl \
		--conf "{\"ds\": \"$$DATE\"}"

# ─── Backfill: ดึงข้อมูลย้อนหลัง ───
backfill:
	docker compose exec airflow-scheduler \
		airflow dags backfill thailand_weather_etl \
		--start-date 2024-01-01 \
		--end-date 2024-01-07

# ─── Run dbt models ───
dbt-run:
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

# ─── Run dbt tests ───
dbt-test:
	docker compose exec airflow-scheduler \
		bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

# ─── Query: top 10 hottest provinces ───
query-hot:
	docker compose exec postgres psql -U weather -d weather_db -c \
		"SELECT province_name, temp_avg, temp_max, total_precipitation, weather_description \
		 FROM mart.daily_summary \
		 WHERE data_date = (SELECT MAX(data_date) FROM mart.daily_summary) \
		 ORDER BY temp_avg DESC LIMIT 10;"

# ─── Query: pipeline run history ───
query-log:
	docker compose exec postgres psql -U weather -d weather_db -c \
		"SELECT data_date, provinces_ok, records_inserted, status, created_at \
		 FROM mart.pipeline_run_log ORDER BY created_at DESC LIMIT 10;"

# ─── Clean all data volumes ───
clean:
	docker compose down -v
	@echo "All volumes removed."

# ─── Install Airflow init ───
init:
	docker compose run --rm airflow-init
