# NYC Taxi ETL Pipeline

## Overview

This project processes NYC taxi data from an open database, transforms it, and loads it into a PostgreSQL database. The final step involves generating a top-10 revenue zone report for further analysis. The pipeline is orchestrated using Apache Airflow for automation.&#x20;

### **Notice**

Since the data in the Data Lake is not actual **the workflow performs 2021 year's data** (It can optionally be changed in PIPELINE.py)

## Data Sources

- **Data Lake:** GitHub repository [DataTalksClub/nyc-tlc-data](https://github.com/DataTalksClub/nyc-tlc-data)
- **Data Warehouse:** PostgreSQL

## Tech Stack

- **ETL Pipeline:** Python
- **Data Processing:** Pandas, PySpark, SQL
- **Orchestration:** Apache Airflow
- **Database:** PostgreSQL
- **Visualization:** pgAdmin
- **Containerization:** Docker + WSL

## Data Structure

Data is provided in three CSV files:

- `yellow_trip_data.csv`
- `green_trip_data.csv`
- `taxi_zone_lookup.csv`

### Key Columns

| Column                 | Yellow Taxi | Green Taxi | Taxi Zones |
| ---------------------- | ----------- | ---------- | ---------- |
| `VendorID`             | ✓           | ✓          |            |
| `pickup_datetime`      | ✓           | ✓          |            |
| `dropoff_datetime`     | ✓           | ✓          |            |
| `Passenger_count`      | ✓           | ✓          |            |
| `Trip_distance`        | ✓           | ✓          |            |
| `RateCodeID`           | ✓           | ✓          |            |
| `Store_and_fwd_flag`   | ✓           | ✓          |            |
| `PULocationID`         | ✓           | ✓          | ✓          |
| `DOLocationID`         | ✓           | ✓          | ✓          |
| `Payment_type`         | ✓           | ✓          |            |
| `Fare_amount`          | ✓           | ✓          |            |
| `Tip_amount`           | ✓           | ✓          |            |
| `Total_amount`         | ✓           | ✓          |            |
| `Congestion_Surcharge` | ✓           | ✓          |            |

## Pipeline Workflow

1. **Download Data:** Extracts monthly NYC taxi data from the data lake.
2. **Load into PostgreSQL:** Parses CSV files and loads them into PostgreSQL.
3. **Data Transformation:**
   - Converts data types and formats using Pandas.
   - Joins location IDs with their actual names within SQL.
4. **Revenue Analysis:**
   - Uses PySpark to compute the top 10 most profitable drop-off zones.
   - Creates tables ranking zones by revenue for the current month.
5. **Orchestration:**
   - Airflow DAG schedules the pipeline to run **on the 1st of each month at 20:25 MSK**.

## Containerization with Docker

Docker Compose is used to containerize the entire pipeline:

- PostgreSQL databases for Airflow and DWH itself
- Airflow environment
- pgAdmin for visualization
- Network setup to interconnect containers

## Deployment Instructions

### **Option 1: One-Time Execution**

1. Run Docker Compose from the `docker` directory to set up the database and pgAdmin:
   ```sh
   docker-compose build
   docker-compose up -d
   ```
2. Execute `PIPELINE.py` manually.

### **Option 2: Orchestrated Execution with Airflow**

1. Run Docker Compose from `docker/airflow`:
   ```sh
   docker-compose build
   docker-compose up -d
   ```
   **Important:** Update the volume path for the DAG folder to match your local path (in Linux format).
2. Access Airflow at `http://localhost:8081`, enable the DAG, and trigger execution.

