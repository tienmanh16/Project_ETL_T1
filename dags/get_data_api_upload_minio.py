from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import oracledb
from plugins.config import config
from datetime import timedelta
from datetime import datetime
import pendulum
from plugins.minio_helper import MinioHelper
from plugins.weather_helper import WeatherHelper
import json
import time


minio = MinioHelper()
weather_api = WeatherHelper() 

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

location = [
    {"city": "HoChiMinh", "lat": 10.823098, "lon": 106.629663},
    {"city": "Hanoi", "lat": 21.027763, "lon": 105.83416},
    {"city": "CanTho", "lat": 10.033333, "lon": 105.766667},
    {"city": "DaNang", "lat": 16.067546, "lon": 108.220325},
]

def get_weather_data():
   for city in location:
      print(city)
      json_data = weather_api.get_current_weather_from_api(city.get('lat'), city.get('lon'))
      timestamp = json_data.get('current').get("dt")

      date = datetime.fromtimestamp(timestamp)

      formatted_date = date.strftime('%Y-%m-%d') #yyyy-mm-dd
      json_data = json.dumps(json_data)
      minio.write_json(bucket_name=config.MINIO_BUCKET, object_name=f'raw_data/{city.get("city")}/{formatted_date}/weather_data_{timestamp}.json', json_data=json_data)

######################################

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email': ['tienmanh1609jike@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dag = DAG(
#     'get_data_api_upload_minio',
#     default_args=default_args,
#     description='Get data from API and upload Minio',
#     schedule='0 10 * * *',  
# )

with DAG(
    'get_data_api_upload_minio',
    default_args=default_args,
    description='Get data from API and upload Minio',
    schedule='0 10 * * *',  
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
) as dag:


   weather_data = PythonOperator(
        task_id='get_weather_data',
        python_callable=get_weather_data,
        dag=dag,
    )


   weather_data