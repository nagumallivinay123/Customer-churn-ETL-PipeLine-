from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
API_CONN_ID = 'open_meteo_api'
POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'start_date': datetime(2024, 7, 8)
}

with DAG(dag_id='weather_london',
         default_args=default_args,
         schedule='@hourly',
         catchup=False,
         tags=['weather']) as dag:

    @task()
    def get_coordinates():
        return {'latitude': LATITUDE, 'longitude': LONGITUDE}

    @task()
    def load_raw_weather(coords):
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f"/v1/forecast?latitude={coords['latitude']}&longitude={coords['longitude']}&hourly=temperature_2m,relative_humidity_2m,precipitation&timezone=auto"
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            data = response.json()
            return {
                'latitude': coords['latitude'],
                'longitude': coords['longitude'],
                'hourly': data['hourly']
            }
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_temperature(raw_data):
        latest_idx = -1 
        return {
             
            'latitude': raw_data['latitude'],
            'longitude': raw_data['longitude'],
            'temperature': raw_data['hourly']['temperature_2m'][latest_idx]
        }

    @task()
    def transform_humidity(raw_data):
        latest_idx = -1 
        return {
            'latitude': raw_data['latitude'],
            'longitude': raw_data['longitude'],
            'humidity': raw_data['hourly']['relative_humidity_2m'][latest_idx]
        }

    @task()
    def transform_precipitation(raw_data):
        latest_idx = -1 
        return {
            'latitude': raw_data['latitude'],
            'longitude': raw_data['longitude'],
            'precipitation': raw_data['hourly']['precipitation'][latest_idx]
        }

    @task()
    def load_final_model(temp_data, humidity_data, precip_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS complex_weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            humidity FLOAT,
            precipitation FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
       
        cursor.execute("""
        INSERT INTO complex_weather_data (latitude, longitude, temperature, humidity, precipitation)
        VALUES (%s, %s, %s, %s, %s)
        """, (
            temp_data['latitude'],
            temp_data['longitude'],
            temp_data['temperature'],
            humidity_data['humidity'],
            precip_data['precipitation']
        ))

        conn.commit()
        cursor.close()

    coords = get_coordinates()
    raw_weather = load_raw_weather(coords)
   

    temp = transform_temperature(raw_weather)
    humidity = transform_humidity(raw_weather)
    precip = transform_precipitation(raw_weather)

    load_final_model(temp, humidity, precip)