from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import snowflake.connector
import requests
import pandas as pd


latitude = Variable.get("LATITUDE")
longitude = Variable.get("LONGITUDE")


def return_snowflake_conn(): 
    """Return the snowflake connection cursor object by initializing the snowflakeHook"""

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()






@task
def extract():
    """Extract the weather data from the API and return it as a json object."""


    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    return response.json()


@task
def transform(data):  
    """Transform the json data and convert it into a list of tuples for insertion.""" 

    df = pd.DataFrame({
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    #insert in load() cannot process df, so we need to convert the data types to plain python values.
    #Nan to None
    df = df.astype(object).where(pd.notnull(df), None)  
    return [tuple(row) for row in df.values.tolist()]


@task
def load(records, target_table):
    """Load the data, perform insert and delete in one transaction to maintain idempotency."""

    con = return_snowflake_conn()
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (latitude FLOAT, longitude FLOAT,
                  date DATE, temp_max FLOAT, temp_min FLOAT, precipitation FLOAT, weather_code VARCHAR(15),
                  PRIMARY KEY (latitude, longitude, date)
                  );
              """)
        con.execute(f"DELETE FROM {target_table};")
        con.executemany(f"INSERT INTO {target_table} VALUES (%s, %s, %s, %s, %s, %s, %s)", records)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id="weather_data_pipeline",
    start_date=datetime(2026, 9, 23),
    catchup=False,
    tags=["ETL"],
    schedule="30 2 * * *"
) as dag:
    target_table = "raw.weather_report"

    data = extract()
    records = transform(data)
    load(records, target_table)




