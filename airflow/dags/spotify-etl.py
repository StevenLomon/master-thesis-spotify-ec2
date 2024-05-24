from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow'
}