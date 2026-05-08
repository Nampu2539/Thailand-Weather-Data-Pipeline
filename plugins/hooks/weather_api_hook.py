"""
plugins/hooks/weather_api_hook.py
==================================
Custom Airflow Hook สำหรับ Open-Meteo API
ใช้ใน DAG แทน requests.get โดยตรง — ดู clean กว่า + มี retry built-in
"""

import time
import logging
from typing import Dict, List, Optional

import requests
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class OpenMeteoHook(BaseHook):
    """
    Custom Hook สำหรับ Open-Meteo API (ฟรี, ไม่ต้อง API key)

    วิธีใช้ใน DAG:
        hook = OpenMeteoHook()
        data = hook.get_weather(lat=13.7563, lon=100.5018, date="2024-01-15")
    """

    conn_name_attr = "openmeteo_conn_id"
    default_conn_name = "openmeteo_default"
    conn_type = "http"
    hook_name = "Open-Meteo Weather API"

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    DEFAULT_HOURLY_VARS = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "wind_direction_10m",
        "weather_code",
        "surface_pressure",
        "cloud_cover",
    ]

    def __init__(
        self,
        openmeteo_conn_id: str = default_conn_name,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        super().__init__()
        self.openmeteo_conn_id = openmeteo_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def get_weather(
        self,
        lat: float,
        lon: float,
        date: str,
        hourly_vars: Optional[List[str]] = None,
    ) -> Dict:
        """
        ดึงข้อมูลสภาพอากาศรายชั่วโมงสำหรับ 1 จุด 1 วัน

        Args:
            lat: Latitude
            lon: Longitude
            date: Date string YYYY-MM-DD
            hourly_vars: รายการ variables ที่ต้องการ (default: 8 ตัวหลัก)

        Returns:
            dict: JSON response จาก Open-Meteo API
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(hourly_vars or self.DEFAULT_HOURLY_VARS),
            "start_date": date,
            "end_date": date,
            "timezone": "Asia/Bangkok",
            "wind_speed_unit": "kmh",
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                log.warning(f"Timeout (attempt {attempt}/{self.max_retries})")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limit
                    wait = self.retry_delay * (2 ** attempt)
                    log.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                else:
                    raise

            except Exception as e:
                log.error(f"API error: {e}")
                raise

        raise RuntimeError(f"Failed after {self.max_retries} retries")

    def get_weather_batch(
        self,
        provinces: List[Dict],
        date: str,
        rate_limit_delay: float = 0.3,
    ) -> List[Dict]:
        """
        ดึงข้อมูลหลาย province พร้อมกัน (sequential + rate limiting)

        Args:
            provinces: list ของ dict มี keys: code, name, lat, lon
            date: Date string YYYY-MM-DD
            rate_limit_delay: วินาทีที่รอระหว่างแต่ละ request

        Returns:
            list ของ (province_info, api_response) tuples
        """
        results = []

        for province in provinces:
            try:
                data = self.get_weather(
                    lat=province["lat"],
                    lon=province["lon"],
                    date=date,
                )
                results.append({
                    "province": province,
                    "data": data,
                    "success": True,
                    "error": None,
                })
                log.info(f"✓ {province.get('name_en', province.get('code'))}")

            except Exception as e:
                results.append({
                    "province": province,
                    "data": None,
                    "success": False,
                    "error": str(e),
                })
                log.error(f"✗ {province.get('name_en')}: {e}")

            time.sleep(rate_limit_delay)

        success = sum(1 for r in results if r["success"])
        log.info(f"Batch complete: {success}/{len(provinces)} successful")
        return results
