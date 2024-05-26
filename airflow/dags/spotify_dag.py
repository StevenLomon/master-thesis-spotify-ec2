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
    df = pd.DataFrame(raw_playlist_data['playlists']['items'])
    now = datetime.now()
    date_now_string = now.strftime('%Y-%m-%d')
    file_str = f"spotify_playlist_raw_data_{date_now_string}"
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    return [output_file_path, file_str, raw_playlist_data]

def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids='tsk_extract_spotify_data')[2]
    object_key_base = task_instance.xcom_pull(task_ids='tsk_extract_spotify_data')[1]
    print("Data and object_key_base pulled from XCom:", data, object_key_base)

    clean_playlist_data = transform_raw_playlist_data(data)
    final_clean_data = transform_data_final(clean_playlist_data)

    # Convert DataFrame to CSV format
    csv_data = clean_playlist_data.to_csv(index=False)

    # Convert DataFrames to Parquet format and store in memory buffer
    parquet_buffer = BytesIO()
    final_clean_data.to_parquet(parquet_buffer, index=False)
    print("Parquet buffer created")

    parquet_buffer.seek(0)  # Rewind the buffer
    print("Parquet buffer seek done")

    # Upload CSV to S3
    object_key = f"{object_key_base}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)

    # Upload paruqet to S3
    object_key = f"spotify_data_final.parquet"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=parquet_buffer.getvalue())
    print("Parquet uploaded to S3")

    # Return refined_tracks.json to be uploaded to S3 via BashOperator
    file_str = "refined_tracks.json"
    output_file_path = f"/home/ubuntu/{file_str}"
    return [output_file_path, file_str]

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
         # schedule_interval = @weekly,
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

        load_json_intermediate_data_to_s3 = BashOperator(
             task_id= 'tsk_load_json_intermediate_data_to_s3',
             bash_command= f'aws s3 mv {{ ti.xcom_pull("tsk_transform_spotify_data")[0] }} s3://spotify-airflow-etl-bucket'
        )

        extract_spotify_data >> transform_spotify_data >> load_json_intermediate_data_to_s3 >> load_raw_data_to_s3