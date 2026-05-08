-- models/marts/mart_monthly_province.sql
-- Monthly aggregation by province — สำหรับ trend analysis

{{ config(materialized='table') }}

SELECT
    province_code,
    province_name,
    region,
    DATE_TRUNC('month', data_date)::DATE AS month,
    TO_CHAR(DATE_TRUNC('month', data_date), 'YYYY-MM') AS month_label,

    -- Temperature
    ROUND(AVG(temp_avg)::numeric, 2)   AS temp_monthly_avg,
    ROUND(MAX(temp_max)::numeric, 2)   AS temp_monthly_max,
    ROUND(MIN(temp_min)::numeric, 2)   AS temp_monthly_min,

    -- Precipitation
    ROUND(SUM(total_precipitation)::numeric, 2) AS total_rain_mm,
    COUNT(CASE WHEN total_precipitation > 0 THEN 1 END)    AS rainy_days,
    COUNT(CASE WHEN total_precipitation >= 10 THEN 1 END)  AS heavy_rain_days,

    -- Wind
    ROUND(AVG(wind_speed_avg)::numeric, 2) AS wind_avg,
    ROUND(MAX(wind_speed_max)::numeric, 2) AS wind_max,

    -- Cloud
    ROUND(AVG(cloud_cover_avg)::numeric, 2) AS cloud_avg,

    -- Summary
    COUNT(*) AS days_recorded,
    ROUND(AVG(data_quality_pct)::numeric, 2) AS avg_quality_pct

FROM {{ source('weather_db', 'daily_summary') }}
GROUP BY 1, 2, 3, 4, 5
ORDER BY province_code, month
