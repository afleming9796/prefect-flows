import requests
import pyarrow as pa
import pyarrow.compute as pc
import duckdb
import datetime
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.blocks.system import Secret

#Load Prefect Blocks 
secret_block_motherduck = Secret.load("motherduck")
secret_block_tomorrow = Secret.load("tomorrow")

#Define database connection 
MD_TOKEN = secret_block_motherduck.get()
MOTHERDUCK_CONN = f'md:my_db?motherduck_token={MD_TOKEN}' 

# Define API Constants
API_KEY = secret_block_tomorrow.get()
LAT, LON = 39.9526, -75.1652  # Philadelphia coordinates
URL = "https://api.tomorrow.io/v4/weather/realtime"

@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=55))
def fetch_weather() -> dict:
    """Fetch real-time weather data from Tomorrow.io API."""
    params = {
        "location": f"{LAT},{LON}",
        "apikey": API_KEY,
        "units": "imperial"
    }
    
    response = requests.get(URL, params=params)
    
    if response.status_code == 200:
        return response.json()["data"]["values"]
    else:
        raise ValueError(f"API request failed: {response.status_code}, {response.text}")


@task
def write_to_motherduck(data: dict) -> pa.Table:
    """Convert JSON weather data to a PyArrow Table with timestamp."""
    columns = list(data.keys())
    values = [data[key] for key in columns]

    # Add a timestamp column (use local time)
    timestamp = datetime.datetime.now().isoformat()
    columns.append("recorded_at")
    values.append(timestamp)

    arrow_table = pa.table({col: [val] for col, val in zip(columns, values)})

    """Write the PyArrow table to a MotherDuck database."""
    con = duckdb.connect(MOTHERDUCK_CONN)

    # In prod, add types to datasets 
    con.sql('CREATE TABLE IF NOT EXISTS weather as SELECT * FROM arrow_table')

    # Append new data into the table if already exist
    con.sql('INSERT INTO weather SELECT * FROM arrow_table')
    
    print("Data written to MotherDuck successfully.")


@flow()
def fetch_weather_data():
    """Prefect flow to fetch, process, and store weather data with timestamps."""
    weather_data = fetch_weather()
    write_to_motherduck(weather_data)