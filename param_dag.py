from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from textwrap import dedent

from datetime import datetime

config = Variable.get('param_dag_config', deserialize_json=True)

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('param_dag',
        schedule_interval='@daily',
        default_args=default_args,
        params=config,
        catchup=False) as dag:

    template = dedent('{{ params }}')

    conf = template if len(template) > 0 else Variable.get('param_dag_config', deserialize_json=True)

    task_1 = BashOperator(
        task_id = 'task_1',
        bash_command='exit 1',
        do_xcom_push = False,
    )

    task_2 = BashOperator(
        task_id = 'task_2',
        bash_command='exit 0',
        do_xcom_push = False,
    )

    task_3 = BashOperator(
        task_id = 'task_3',
        bash_command=f'echo {conf}; exit 0',
        do_xcom_push = False,
        trigger_rule='all_done'
    )

    task_4 = BashOperator(
        task_id = 'task_4',
        bash_command='exit 0',
        do_xcom_push = False,
        trigger_rule='one_failed'
    )

    [task_1, task_2] >> task_3
    [task_1, task_2] >> task_4

