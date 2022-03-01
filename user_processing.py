# From Marc Lambertis' Airflow course on Udemy

from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash_operator import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021,1,1)
}

with DAG('user_processing', 
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:
    # Define tasks/operators

#   bash_task = BashOperator(
#       task_id='worker_sleep',
#       bash_command='sleep 120'
#   )
    
    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
                );
            '''
    )

    # airflow connections add 'user_api' --conn-type 'http' --host https://randomuser.me/api/ 
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )
    
#   bash_task >> creating_table


