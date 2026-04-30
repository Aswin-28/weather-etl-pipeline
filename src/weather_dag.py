from airflow import DAG

from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

import requests

import pandas as pd

import os

from sqlalchemy import create_engine

# --- Config ---

API_KEY = "753db2d3a484eb80c309555593558cc0"

CITIES = ["Bengaluru", "Mumbai", "Chennai", "Delhi", "Hyderabad"]

DB_USER = "postgres"

DB_PASSWORD = "c1a931163d2744bd8de90bfbee931413"

DB_HOST = "172.20.0.1"

DB_PORT = "5432"

DB_NAME = "weather_pipeline"

# --- Extract ---

def fetch_all_weather():

    all_data = []

    for city in CITIES:

        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

        response = requests.get(url)

        data = response.json()

        all_data.append({

            'city'        : data['name'],

            'temperature' : data['main']['temp'],

            'feels_like'  : data['main']['feels_like'],

            'humidity'    : data['main']['humidity'],

            'weather'     : data['weather'][0]['description'],

            'wind_speed'  : data['wind']['speed'],

            'timestamp'   : pd.Timestamp.now().isoformat()

        })

    return all_data

# --- Transform ---

def transform_weather():

    all_data = fetch_all_weather()

    df = pd.DataFrame(all_data)

    # Save Parquet

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

    os.makedirs("/home/aswin/weather_data/parquet", exist_ok=True)

    os.makedirs("/home/aswin/weather_data/csv", exist_ok=True)

    df.to_parquet(f"/home/aswin/weather_data/parquet/weather_{timestamp}.parquet", index=False)

    # Append CSV

    csv_path = "/home/aswin/weather_data/csv/clean_data.csv"

    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode="a", header=False, index=False)

    else:

        df.to_csv(csv_path, index=False)

    print(f"Saved data for {len(df)} cities")

    return all_data

# --- Load ---

def load_to_postgres():
    import psycopg2
    all_data = fetch_all_weather()
    df = pd.DataFrame(all_data)
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(100),
            temperature FLOAT,
            feels_like FLOAT,
            humidity INT,
            weather VARCHAR(200),
            wind_speed FLOAT,
            timestamp VARCHAR(50)
        )
    """)
    
    # Insert rows
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO weather_data 
            (city, temperature, feels_like, humidity, weather, wind_speed, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['city'],
            row['temperature'],
            row['feels_like'],
            row['humidity'],
            row['weather'],
            row['wind_speed'],
            row['timestamp']
        ))
    
    conn.commit()
    cursor.execute("SELECT current_database();")
    print(cursor.fetchone())  # this will tell us exactly which DB it's inserting into
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows into PostgreSQL")

# --- DAG Definition ---

default_args = {

    'owner': 'aswin',

    'retries': 1,

    'retry_delay': timedelta(minutes=5)

}

with DAG(

    dag_id="weather_etl_pipeline",

    default_args=default_args,

    description="Hourly weather ETL for 5 Indian cities",

    schedule="@hourly",

    start_date=datetime(2026, 4, 29),

    catchup=False

) as dag:

    task_transform = PythonOperator(

        task_id="fetch_and_transform",

        python_callable=transform_weather

    )

    task_load = PythonOperator(

        task_id="load_to_postgres",

        python_callable=load_to_postgres

    )

    task_transform >> task_load
