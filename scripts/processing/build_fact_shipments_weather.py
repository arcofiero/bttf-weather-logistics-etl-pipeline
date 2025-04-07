import os
import logging
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

load_dotenv()
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/fact_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

PG_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD")
}

WEATHER_CSV_PATH = "data/raw/weather/weather_data_2022_07.csv"


# -------------------------------------------------------------------
# Database Utilities
# -------------------------------------------------------------------

def get_sqlalchemy_engine():
    """Creates and returns a SQLAlchemy engine."""
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        host=PG_CONFIG["host"],
        port=PG_CONFIG["port"],
        database=PG_CONFIG["dbname"]
    )
    return create_engine(url)


def load_shipments_and_cities():
    """Fetches shipment and city data from PostgreSQL."""
    try:
        engine = get_sqlalchemy_engine()
        shipments_df = pd.read_sql("SELECT * FROM shipments.shipments", con=engine)
        cities_df = pd.read_sql("SELECT * FROM shipments.cities", con=engine)
        logging.info("Successfully loaded shipments and cities from database.")
        return shipments_df, cities_df
    except SQLAlchemyError as e:
        logging.error(f"Database load error: {e}")
        return pd.DataFrame(), pd.DataFrame()


# -------------------------------------------------------------------
# File I/O
# -------------------------------------------------------------------

def load_weather_csv(filepath):
    """Loads weather data from a CSV file."""
    if not os.path.exists(filepath):
        logging.error(f"Weather CSV not found: {filepath}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(filepath)
        logging.info(f"Loaded weather data: {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"Error reading weather CSV: {e}")
        return pd.DataFrame()


# -------------------------------------------------------------------
# Data Processing
# -------------------------------------------------------------------

def preprocess_and_join(shipments_df, cities_df, weather_df):
    """
    Cleans and merges shipment, city, and weather data to form a fact table.
    """
    if shipments_df.empty or cities_df.empty or weather_df.empty:
        logging.warning("One or more input datasets are empty.")
        return pd.DataFrame()

    cities_df["name"] = cities_df["name"].str.strip().str.lower()
    weather_df["city"] = weather_df["city"].str.strip().str.lower()

    merged_weather = pd.merge(
        weather_df,
        cities_df,
        how="left",
        left_on=["city", "latitude", "longitude"],
        right_on=["name", "latitude", "longitude"]
    )

    shipments_df["shipment_start_timestamp"] = pd.to_datetime(shipments_df["shipment_start_timestamp"])
    shipments_df["hourly_timestamp"] = shipments_df["shipment_start_timestamp"].dt.floor("h")
    shipments_df["city"] = shipments_df["start_location"].str.strip().str.lower()

    merged_weather["timestamp"] = pd.to_datetime(merged_weather["timestamp"])
    merged_weather["hourly_timestamp"] = merged_weather["timestamp"].dt.floor("h")

    fact_df = pd.merge(
        shipments_df,
        merged_weather,
        how="inner",
        on=["city", "hourly_timestamp"]
    )

    if fact_df.empty:
        logging.warning("No matching records found after join.")
        return pd.DataFrame()

    result_df = fact_df[[
        "id_x",  # shipment_id
        "id_y",  # city_id
        "shipment_start_timestamp",
        "consumed_fuel",
        "temperature_2m",
        "windspeed_10m",
        "precipitation",
        "weathercode"
    ]].rename(columns={
        "id_x": "shipment_id",
        "id_y": "city_id",
        "shipment_start_timestamp": "timestamp",
        "consumed_fuel": "fuel_consumed_liters"
    })

    logging.info(f"Final fact table shape: {result_df.shape}")
    return result_df


# -------------------------------------------------------------------
# Output Writer
# -------------------------------------------------------------------

def write_fact_table_to_csv(df):
    """Writes the fact table to a CSV file."""
    if df.empty:
        logging.warning("Fact table is empty. CSV write skipped.")
        return

    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/fact_shipments_weather.csv"
    df.to_csv(output_path, index=False)
    logging.info(f"Fact table written to: {output_path}")


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------

def main():
    logging.info("ETL pipeline started.")

    shipments_df, cities_df = load_shipments_and_cities()
    weather_df = load_weather_csv(WEATHER_CSV_PATH)

    final_df = preprocess_and_join(shipments_df, cities_df, weather_df)
    write_fact_table_to_csv(final_df)

    logging.info("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()