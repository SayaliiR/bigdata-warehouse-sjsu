WEATHER DATA PIPELINE (OPEN-MATEO API -> SNOWFLAKE CONNECTION -> AIRFLOW)


This is an Airflow ETL pipeline that pulls 60 days of weather data for San Diego city from OPEN-MATEO API and loads it into snowflake database. 

Each run performs full refresh (DELETE + INSERT)  in one transaction ensuring Idempotency.