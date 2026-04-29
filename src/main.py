import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")
CITY = "Bengaluru"

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def fetch_data(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    print(f"Status: {response.status_code}")
    return response.json()

def parse_data(data):
    print("data is getting parsed")
    return pd.DataFrame([{
        'city' : data['name'],
        'temperature' : data['main']['temp'],
        'feels_like' : data['main']['feels_like'],
        'humidity' : data['main']['humidity'],
        'weather' : data['weather'][0]['description'],
        'wind_speed' : data['wind']['speed'],
        'timestamp' : pd.Timestamp.now()
    }])

def save_files(df):
    df.to_csv("./static/clean_data.csv", index=False)
    df.to_parquet("./static/clean_parq.parquet", index=False)
    print("data is getting saved")

def load_to_postgres(df):
    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    df.to_sql(
        name="weather_data",
        con=engine,
        if_exists="append",
        index=False
    )
    print("Data loaded into PostgreSQL successfully")

# --- Run ---
data = fetch_data(CITY, API_KEY)
df = parse_data(data)
print(df)
save_files(df)
load_to_postgres(df)