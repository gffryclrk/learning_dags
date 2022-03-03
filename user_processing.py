# From Marc Lambertis' Airflow course on Udemy

from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pandas import json_normalize

from datetime import datetime
import json

default_args = {
    'start_date': datetime(2021,1,1)
}

def _processing_user(ti):
    users = ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/opt/airflow/data/tmp/processed_user.csv', index=None, header=False)




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

    # airflow connections add 'user_api' --conn-type 'http' --host https://randomuser.me/
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    check_temp_data_dir = BashOperator(
        task_id='check_temp_data_dir',
        bash_command='[[ -d /opt/airflow/data/tmp ]] || mkdir /opt/airflow/data/tmp'
    )


    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=_processing_user
    )

    storing_user = BashOperator(
        task_id='storing_user',
        bash_command='echo -e ".separator ","\n.import /opt/airflow/data/tmp/processed_user.csv users" | sqlite3 /opt/airflow/data/test.db'
    )

    creating_table >> is_api_available >> extracting_user >> check_temp_data_dir >> processing_user >> storing_user
