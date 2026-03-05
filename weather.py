from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests
import os
from datetime import datetime, timezone
import json
import pandas as pd
import time

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
API_KEY = os.getenv('OPENWEATHER_API_KEY')
API_BASE_URL = 'https://api.openweathermap.org/data/2.5'
DEFAULT_LAT = float(os.getenv('DEFAULT_LAT', 39.95))
DEFAULT_LON = float(os.getenv('DEFAULT_LON', -75.16))
DEFAULT_CITY = os.getenv('DEFAULT_CITY', 'Philadelphia')
CITIES_FILE = os.getenv('CITIES_FILE', 'cities.csv')

def get_engine():
    return create_engine(DATABASE_URL)

def create_table():
    create_sql = text("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            feels_like FLOAT,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed FLOAT,
            timestamp TIMESTAMP,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    with get_engine().connect() as conn:
        conn.execute(create_sql)
        conn.commit()

    print("\nTable weather_data exists in Database weather_db\n")

def health():
    try:
        with get_engine().connect() as conn:
            conn.execute(text('SELECT 1'))
        db_status = 'connected'
    except Exception as e:
        db_status = f'error: {str(e)}'

    print(f"\nstatus: {db_status}\ntimestamp: {datetime.now(timezone.utc)}\n")

def collect_weather_display():
    lat = DEFAULT_LAT
    lon = DEFAULT_LON
    city = DEFAULT_CITY
    try:        
        print(f"\nFetching weather for: ({lat}, {lon}, {city})")
        response = requests.get(
            f"{API_BASE_URL}/weather",
            params={
                #'name': city,
                'lat': lat,
                'lon': lon,
                'appid': API_KEY
            },
            timeout=10
        )
        response.raise_for_status()
        weather_data = response.json()        
        print(f"\nsucess: {True}\nresponse:\n{json.dumps(weather_data, indent=4)}")
    except Exception as e:
        print(e)
    return weather_data

def store_weather_data(data):
    insert_sql = text("""
        INSERT INTO weather_data
            (city, temperature, feels_like, humidity, pressure, wind_speed, timestamp)
        VALUES
            (:city, :temperature, :feels_like, :humidity, :pressure, :wind_speed, :timestamp)
    """)

    params = {
        'city': data['name'],
        'temperature': data['main']['temp'] - 273.15,
        'feels_like': data['main']['feels_like'] - 273.15,
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'wind_speed': data['wind']['speed'],
        'timestamp': datetime.fromtimestamp(data['dt'], tz=timezone.utc)
    }
    
    with get_engine().connect() as conn:
        conn.execute(insert_sql, params)
        conn.commit()

    print(f"\nWeather datastored for {params['city']}")

def query_all():
    with get_engine().connect() as conn:
        df = pd.read_sql(text("SELECT * FROM weather_data;"), conn)
    print(df.to_string())

def collect_batch():
    cities = pd.read_csv(CITIES_FILE)

    success = 0
    failed = 0

    for _, row in cities.iterrows():
        try:
            print(f"\nFetching weather for {row['city']}")
            response = requests.get(
                f"{API_BASE_URL}/weather",
                params={
                    'lat': row['lat'],
                    'lon': row['lon'],
                    'appid': API_KEY
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            store_weather_data(data)
            success += 1
            time.sleep(1.5)
        
        except Exception as e:
            print(f"Failed for {row['city']}: {e}")
            failed += 1
    
    print(f"\nBatch complete. Success: {success} | Failed: {failed}")






