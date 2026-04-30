# weather-etl-pipeline
End-to-end ETL pipeline that ingests live weather data from OpenWeatherMap API, transforms with Python/pandas, and loads into PostgreSQL, CSV, and Parquet

## Pipeline Architecture
OpenWeatherMap API
↓
Extract (requests)
↓
Transform (pandas)
↓
┌───────────────────┐
│ CSV file          │
│ Parquet file      │
│ PostgreSQL DB     │
└───────────────────┘

## Tech Stack

- **Python** — core pipeline logic
- **pandas** — data transformation
- **requests** — API ingestion
- **SQLAlchemy + psycopg2** — PostgreSQL connection
- **PostgreSQL** — relational database storage
- **Parquet** — columnar file format for efficient storage

## Orchestration

This pipeline is orchestrated using **Apache Airflow** running on WSL2.

- DAG: `weather_etl_pipeline`
- Schedule: `@hourly`
- Tasks:
  - `fetch_and_transform` — hits API for 5 cities, saves CSV and Parquet
  - `load_to_postgres` — inserts data into PostgreSQL

weather-etl-pipeline/
├── src/
│   ├── main.py           # Manual ETL script
│   └── weather_dag.py    # Airflow DAG for scheduled pipeline
├── .gitignore
├── .env.example
└── README.md

## What This Pipeline Does

1. **Extract** — Hits the OpenWeatherMap API and pulls current weather data for a configured city
2. **Transform** — Parses the raw JSON response into a structured pandas DataFrame
3. **Load** — Saves the cleaned data to:
   - A CSV file for human readability
   - A Parquet file for efficient columnar storage
   - A PostgreSQL database table for querying and analysis

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/Aswin-28/weather-etl-pipeline.git
cd weather-etl-pipeline
```

### 2. Install dependencies
```bash
pip install requests pandas pyarrow sqlalchemy psycopg2-binary python-dotenv
```

### 3. Configure environment variables

Create a `.env` file in the root directory:
API_KEY=your_openweathermap_api_key
DB_USER=your_db_username
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_pipeline

### 4. Create the database
```sql
CREATE DATABASE weather_pipeline;
```

### 5. Run the pipeline
```bash
python main.py
```

## Sample Output
Status: 200
city  temperature  feels_like  humidity       weather  wind_speed                 timestamp
0  Bengaluru        31.13        29.5        65   few clouds       10.28   2026-04-26 20:20:04
Data saved as CSV and Parquet
Data loaded into PostgreSQL successfully

## Key Concepts Demonstrated

- ETL pipeline design pattern
- REST API ingestion
- JSON parsing and transformation
- Columnar vs row-based storage formats (Parquet vs CSV)
- Loading data into a relational database
- Secure credential management with environment variables

## Author

Aswin
