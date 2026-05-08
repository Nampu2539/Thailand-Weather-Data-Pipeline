-- models/staging/stg_weather_hourly.sql
-- View ของ staging.weather_hourly พร้อม derived columns

{{ config(materialized='view') }}

SELECT
    province_code,
    province_name,
    latitude,
    longitude,
    measured_at,
    data_date,
    temperature_2m,
    humidity,
    precipitation,
    wind_speed,
    wind_direction,
    weather_code,
    surface_pressure,
    cloud_cover,
    is_valid,
    -- Derived columns
    CASE
        WHEN temperature_2m >= 35 THEN 'Very Hot'
        WHEN temperature_2m >= 30 THEN 'Hot'
        WHEN temperature_2m >= 25 THEN 'Warm'
        WHEN temperature_2m >= 20 THEN 'Comfortable'
        ELSE 'Cool'
    END AS temp_category,
    CASE
        WHEN precipitation >= 10 THEN 'Heavy Rain'
        WHEN precipitation >= 2.5 THEN 'Moderate Rain'
        WHEN precipitation > 0 THEN 'Light Rain'
        ELSE 'No Rain'
    END AS rain_category,
    EXTRACT(HOUR FROM measured_at) AS hour_of_day,
    TO_CHAR(measured_at, 'HH24:00') AS time_label
FROM {{ source('weather_db', 'weather_hourly') }}
WHERE is_valid = TRUE
