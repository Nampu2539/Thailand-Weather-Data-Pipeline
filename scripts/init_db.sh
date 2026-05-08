#!/bin/bash
# สร้าง weather_db และ user สำหรับ Data Warehouse
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- สร้าง database และ user สำหรับ Weather DWH
    CREATE USER weather WITH PASSWORD 'weather';
    CREATE DATABASE weather_db OWNER weather;
    GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather;
EOSQL

# สร้าง schema ทั้งหมดใน weather_db
psql -v ON_ERROR_STOP=1 --username weather --dbname weather_db <<-EOSQL

    -- ─── RAW LAYER: เก็บ JSON ดิบจาก API ───
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.weather_responses (
        id              BIGSERIAL PRIMARY KEY,
        province_code   VARCHAR(10) NOT NULL,
        province_name   VARCHAR(100) NOT NULL,
        latitude        DECIMAL(8,5) NOT NULL,
        longitude       DECIMAL(8,5) NOT NULL,
        response_json   JSONB NOT NULL,
        fetched_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        data_date       DATE NOT NULL,
        dag_run_id      VARCHAR(255)
    );

    CREATE INDEX idx_raw_province_date ON raw.weather_responses(province_code, data_date);

    -- ─── STAGING LAYER: ข้อมูล typed + clean ───
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.weather_hourly (
        id              BIGSERIAL PRIMARY KEY,
        province_code   VARCHAR(10) NOT NULL,
        province_name   VARCHAR(100) NOT NULL,
        latitude        DECIMAL(8,5) NOT NULL,
        longitude       DECIMAL(8,5) NOT NULL,
        measured_at     TIMESTAMP WITH TIME ZONE NOT NULL,
        data_date       DATE NOT NULL,
        temperature_2m  DECIMAL(5,2),       -- °C
        humidity        INTEGER,            -- %
        precipitation   DECIMAL(6,2),       -- mm
        wind_speed      DECIMAL(6,2),       -- km/h
        wind_direction  INTEGER,            -- degrees
        weather_code    INTEGER,            -- WMO code
        surface_pressure DECIMAL(7,2),      -- hPa
        cloud_cover     INTEGER,            -- %
        is_valid        BOOLEAN DEFAULT TRUE,
        loaded_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE (province_code, measured_at)
    );

    CREATE INDEX idx_staging_province_date ON staging.weather_hourly(province_code, data_date);
    CREATE INDEX idx_staging_measured_at ON staging.weather_hourly(measured_at);

    -- ─── MART LAYER: Aggregated สำหรับ Dashboard ───
    CREATE SCHEMA IF NOT EXISTS mart;

    CREATE TABLE IF NOT EXISTS mart.daily_summary (
        id                  BIGSERIAL PRIMARY KEY,
        province_code       VARCHAR(10) NOT NULL,
        province_name       VARCHAR(100) NOT NULL,
        latitude            DECIMAL(8,5) NOT NULL,
        longitude           DECIMAL(8,5) NOT NULL,
        region              VARCHAR(50),
        data_date           DATE NOT NULL,
        temp_avg            DECIMAL(5,2),
        temp_max            DECIMAL(5,2),
        temp_min            DECIMAL(5,2),
        humidity_avg        DECIMAL(5,2),
        total_precipitation DECIMAL(8,2),
        wind_speed_avg      DECIMAL(6,2),
        wind_speed_max      DECIMAL(6,2),
        dominant_weather    INTEGER,
        weather_description VARCHAR(100),
        cloud_cover_avg     DECIMAL(5,2),
        hours_recorded      INTEGER,
        data_quality_pct    DECIMAL(5,2),
        updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE (province_code, data_date)
    );

    CREATE INDEX idx_mart_date ON mart.daily_summary(data_date);
    CREATE INDEX idx_mart_province ON mart.daily_summary(province_code);
    CREATE INDEX idx_mart_region ON mart.daily_summary(region);

    -- Data Quality Log
    CREATE TABLE IF NOT EXISTS mart.pipeline_run_log (
        id              BIGSERIAL PRIMARY KEY,
        dag_run_id      VARCHAR(255),
        data_date       DATE NOT NULL,
        step            VARCHAR(50),
        provinces_total INTEGER,
        provinces_ok    INTEGER,
        records_inserted INTEGER,
        records_failed  INTEGER,
        duration_sec    DECIMAL(10,2),
        status          VARCHAR(20),
        error_msg       TEXT,
        created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    GRANT ALL ON ALL TABLES IN SCHEMA raw TO weather;
    GRANT ALL ON ALL TABLES IN SCHEMA staging TO weather;
    GRANT ALL ON ALL TABLES IN SCHEMA mart TO weather;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA raw TO weather;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA staging TO weather;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA mart TO weather;

    RAISE NOTICE 'Weather DWH schema created successfully!';
EOSQL

echo "Database initialization complete."
