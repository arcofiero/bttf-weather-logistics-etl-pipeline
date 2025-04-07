import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# --------------------------------------------------
# Environment Setup
# --------------------------------------------------

load_dotenv()

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/load_fact_table.log",
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

def get_sqlalchemy_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        host=PG_CONFIG["host"],
        port=PG_CONFIG["port"],
        database=PG_CONFIG["dbname"]
    )
    return create_engine(url)

# --------------------------------------------------
# Load CSV into Database
# --------------------------------------------------

def load_fact_table_to_postgres(csv_path):
    if not os.path.exists(csv_path):
        logging.error(f"CSV file not found: {csv_path}")
        return

    engine = get_sqlalchemy_engine()

    try:
        with engine.begin() as conn:
            # Run schema creation separately
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
            logging.info("Ensured schema 'analytics' exists.")

            # Then create the table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytics.fact_shipments_weather (
                    shipment_id BIGINT,
                    city_id BIGINT,
                    timestamp TIMESTAMP,
                    fuel_consumed_liters FLOAT,
                    temperature_2m FLOAT,
                    windspeed_10m FLOAT,
                    precipitation FLOAT,
                    weathercode BIGINT
                );
            """))
            logging.info("Ensured table 'analytics.fact_shipments_weather' exists.")

        # Load CSV into DataFrame
        df = pd.read_csv(csv_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df.to_sql(
            "fact_shipments_weather",
            con=engine,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

        logging.info(f"Loaded {len(df)} rows into analytics.fact_shipments_weather")
        print("Fact table loaded successfully.")

    except Exception as e:
        logging.error(f"Error during load: {e}")
        print(f"Error: {e}")

# --------------------------------------------------
# Entrypoint
# --------------------------------------------------

if __name__ == "__main__":
    CSV_PATH = "data/processed/fact_shipments_weather.csv"
    load_fact_table_to_postgres(CSV_PATH)