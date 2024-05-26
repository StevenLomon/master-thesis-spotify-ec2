import boto3, fastparquet
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from io import BytesIO
from spotify_etl import extract_raw_playlist_data, transform_raw_playlist_data, transform_data_final

s3_client = boto3.client('s3')
target_bucket_name = 'spotify-airflow-etl-bucket'

def extract_data():
    raw_playlist_data = extract_raw_playlist_data()
    df = pd.DataFrame(raw_playlist_data['tracks']['items'])
    now = datetime.now()
    date_now_string = now.strftime('%Y-%m-%d')
    file_str = f"spotify_global50_raw_data_{date_now_string}"
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    return [output_file_path, file_str]

def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids='tsk_extract_spotify_data')[0]
    object_key_base = task_instance.xcom_pull(task_ids='tsk_extract_spotify_data')[1]

    clean_global50_data = transform_raw_playlist_data(data)

    # Convert DataFrame to CSV format
    csv_data = clean_global50_data.to_csv(index=False)

    # Upload CSV to S3
    object_key = f"{object_key_base}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25),
    'email': ['steven.lennartsson@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('spotify_etl_dag',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup=False) as dag:
    
        extract_spotify_data = PythonOperator(
            task_id= 'tsk_extract_spotify_data',
            python_callable=extract_data
        )

        transform_spotify_data = PythonOperator(
             task_id= 'tsk_transform_spotify_data',
             python_callable=transform_data
        )

        load_raw_data_to_s3 = BashOperator(
            task_id= 'tsk_load_raw_data_to_s3',
            bash_command= 'aws s3 mv {{ ti.xcom_pull("tsk_extract_spotify_data")[0] }} s3://spotify-airflow-etl-bucket-raw'
        )

        extract_spotify_data >> transform_spotify_data >> load_raw_data_to_s3