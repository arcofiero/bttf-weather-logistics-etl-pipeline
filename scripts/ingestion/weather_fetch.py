# -----------------------
# Imports and Setup
# -----------------------

import requests
import os
import pandas as pd
import psycopg2
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from tqdm import tqdm

# -----------------------
# Configuration
# -----------------------

# Load environment variables from .env file
load_dotenv()

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    filename='logs/weather.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -----------------------
# Define Open-Meteo parameters
# -----------------------

WEATHER_PARAMS = [
    "temperature_2m",
    "windspeed_10m",
    "precipitation",
    "weathercode"
]
START_DATE = "2022-07-01"
END_DATE = "2022-07-31"

# Load PostgreSQL connection parameters
PG_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD")
}

# -----------------------
# Weather API Fetch Logic
# -----------------------

def fetch_weather_for_city(city_info, retries=3, delay=2):
    """
    Fetch hourly weather data from Open-Meteo API for a single city.
    Includes retry logic for transient failures.
    """
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city_info["latitude"],
        "longitude": city_info["longitude"],
        "hourly": ",".join(WEATHER_PARAMS),
        "start_date": START_DATE,
        "end_date": END_DATE,
        "timezone": "auto"
    }

    for attempt in range(retries):
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            hourly_data = data.get("hourly", {})
            time_series = hourly_data.get("time", [])
            if not time_series:
                raise ValueError("No hourly data received.")

            return [
                {
                    "city": city_info["city"],
                    "latitude": city_info["latitude"],
                    "longitude": city_info["longitude"],
                    "timestamp": timestamp,
                    **{param: hourly_data.get(param, [None])[i] for param in WEATHER_PARAMS}
                }
                for i, timestamp in enumerate(time_series)
            ]

        except Exception as e:
            logging.warning(f"[Attempt {attempt+1}] Failed for {city_info['city']}: {e}")
            time.sleep(delay)

    logging.error(f"Final failure for {city_info['city']}")
    return []

# -----------------------
# Database Access
# -----------------------

def fetch_cities_from_db():
    """
    Connect to PostgreSQL and retrieve cities with latitude/longitude for weather querying.
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        query = """
            SELECT name, latitude, longitude
            FROM shipments.cities
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        cities = [
            {"city": row[0], "latitude": row[1], "longitude": row[2]}
            for row in results
        ]

        cursor.close()
        conn.close()

        logging.info(f"Fetched {len(cities)} cities from database.")
        return cities

    except Exception as e:
        logging.error(f"Database connection or query failed: {e}")
        return []

# -----------------------
# Main Pipeline Function
# -----------------------

def collect_and_store_weather_data():
    """
    Main function to fetch weather data for all cities and save to CSV.
    """
    cities = fetch_cities_from_db()
    if not cities:
        logging.warning("No cities to process. Exiting.")
        return

    all_weather_records = []

    for city in tqdm(cities, desc="Fetching Weather Data"):
        city_weather = fetch_weather_for_city(city)
        if city_weather:
            all_weather_records.extend(city_weather)

    if not all_weather_records:
        logging.warning("No weather data was collected.")
        return

    df = pd.DataFrame(all_weather_records)

    # Ensure output directory exists
    output_dir = "data/raw/weather/"
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{output_dir}/weather_data_2022_07.csv"
    df.to_csv(output_file, index=False)
    logging.info(f"Weather data saved to {output_file}")

# -----------------------
# Entry Point
# -----------------------

if __name__ == "__main__":
    collect_and_store_weather_data()
    print("Connecting to DB:", PG_CONFIG)