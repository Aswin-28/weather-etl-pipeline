import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")
CITIES = ["Bengaluru", "Chennai", "Coimbatore", "Hyderabad", "Kochi"]

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
    # CSV — append if exists
    csv_path = "./data/csv/clean_data.csv"
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode="a", header=False, index=False)
    else:
        df.to_csv(csv_path, index=False)
    print("CSV saved")

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
all_data = []
for CITY in CITIES:
    data = fetch_data(CITY, API_KEY)
    df = parse_data(data)
    print(df)
    save_files(df)
    all_data.append(df)
    load_to_postgres(df)

final_df = pd.concat(all_data, ignore_index=True)
timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
final_df.to_parquet(f"./data/parquet/weather_all_{timestamp}.parquet", index=False)