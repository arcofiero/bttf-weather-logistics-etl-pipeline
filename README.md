# BTTF Weather-Enhanced Logistics ETL Pipeline

A complete Data Engineering case study designed to solve real-world logistics optimization using weather data, PostgreSQL, and Python-based ETL. This project covers everything from architecture design to production-grade pipelines, analytical modeling, and visualization planning.

---

## Problem Statement

Logistics operations are often impacted by external environmental factors, particularly weather conditions. The goal of this project is to simulate a scenario where a logistics company integrates hourly weather data with its shipment records to derive actionable insights such as:

- Fuel efficiency trends under different weather conditions
- Weather impact on delivery patterns
- Identifying peak and optimal delivery hours
- Building a scalable data pipeline to automate ingestion, processing, and reporting

---

## Architecture Overview

The project is structured across **5 incremental levels**:

| Level | Focus Area                      | Key Deliverables |
|-------|----------------------------------|------------------|
| 0     | Assignment Breakdown            | Tracker + Scope Mapping |
| 1     | Solution Architecture           | High-level Diagram + Stack Choices |
| 2     | Weather Data Ingestion          | Python API Scripts + CSV Writer |
| 3     | Data Modeling                   | ERD, Schema Design, Analytical Use Cases |
| 4     | ETL Pipeline + Fact Table Build | Join Logic, Cleaning, Aggregation |
| 5     | Visualization Planning          | Tool Stack, Chart Types, Dashboard Drafts |

---

## Technologies Used

- **PostgreSQL** (Local via DBeaver)
- **Python** for API, ETL scripting, CSV, and SQL ingestion
- **Pandas**, **SQLAlchemy**, **psycopg2**, **tqdm**
- **Draw.io** for Architecture Diagrams
- **GitHub Actions** (CI-ready setup)
- **Obsidian** for documentation

---

## Project Setup Instructions

```bash
# Clone the repo
cd ~/Desktop/DE_Projects/
git clone https://github.com/arcofiero/bttf-weather-logistics-etl-pipeline.git
cd bttf-weather-logistics-etl-pipeline

# Create a virtualenv and install dependencies
pip install -r requirements.txt

# Configure environment variables in `.env`
PG_HOST=localhost
PG_PORT=5432
PG_DB=bttf_assignment
PG_USER=postgres
PG_PASSWORD=your_password
```

---

## Project Workflow Walkthrough

### 1. **Weather Ingestion**
- API: Open-Meteo Archive API
- Time Period: July 2022 (aligned with shipment data)
- Fetch for all cities (from `shipments.cities`)
- Stored as `weather_data_2022_07.csv`

### 2. **Fact Table Creation**
- ETL logic merges weather and shipment data
- Aligns hourly timestamps, maps lat/lon + city_id
- Output: `fact_shipments_weather` table (under `analytics` schema)

### 3. **PostgreSQL Integration**
- Weather data saved as CSV → loaded to PostgreSQL
- Table auto-created using SQLAlchemy
- Schema constraints, column types handled

---

## Sample Analytical Queries Implemented

| Use Case | Description |
|----------|-------------|
| Avg. Fuel Consumed per City | Aggregated fuel efficiency by city |
| Hottest Hour Per City | Identify hottest timestamp in July by city |
| Rain Impact | Fuel efficiency drop during precipitation periods |
| Windspeed vs Fuel | Correlation check between wind and fuel |
| Daily Summaries | City-wise weather aggregations per day |
---

## Data Model Design

### Fact Table
- `fact_shipments_weather`
  - `shipment_id`, `city_id`, `timestamp`, `fuel_consumed_liters`
  - `temperature_2m`, `windspeed_10m`, `precipitation`, `weathercode`

### Dimension Tables
- `dim_city`: name, lat, lon, city_id
- `dim_weather_code`: weather_code, description

ERD included in `/docs/` and walkthrough folder.

---

## Key Challenges & Solutions

| Challenge | Resolution |
|----------|------------|
| Weather data not aligning with shipment timestamps | Applied hourly flooring and timezone harmonization |
| City mismatches | Lowercased and normalized lat/lon joins |
| Empty joins during early stages | Debugged ranges + included city-wise logging |
| PostgreSQL schema handling | Used SQLAlchemy for auto DDL generation |
| Visualization planning | Drafted BI layout before implementation |

---

## Scalability & Monitoring Principles

- **Scalability**:
  - Modular ETL scripts → can swap APIs or DBs easily
  - Clear layering of ingestion → processing → load
  - Ready for orchestration using Airflow, GitHub Actions or cron

- **Monitoring**:
  - Logs captured per city/API call
  - Row counts and errors logged at every stage
  - `.env` secured via `.gitignore`

---

## Learnings & Takeaways

- Real-world data engineering involves frequent debugging of data mismatches
- Joining time-series and spatial data requires careful preprocessing
- Python + SQLAlchemy + PostgreSQL is a powerful lightweight stack for analytics
- GitHub workflows and directory structuring are key for collaborative data projects

---

## Project Structure

```bash
├── data/                      # Raw and processed data
├── docs/                      # All architecture diagrams + ERD
├── logs/                      # Script logs for ingestion and processing
├── scripts/                   # ETL scripts and loaders
├── Project_Walkthrough_Report/  # Level 0–5 PDFs
├── .env
├── requirements.txt
├── .gitignore
├── LICENSE
├── README.md
```

---

## Final Notes

This project simulates real-world thinking: from data collection to business insight delivery. It demonstrates ownership of an end-to-end ETL pipeline integrated with cloud-like APIs, local warehousing, and structured documentation.

For a full breakdown, check the `/Project_Walkthrough_Report/` folder which includes the 6-level modular PDF walkthroughs from design to dashboard planning.

---

> **Author:** Archit Raj ([@arcofiero](https://github.com/arcofiero))
