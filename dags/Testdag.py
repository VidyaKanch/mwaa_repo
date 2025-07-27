from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

with DAG(
        dag_id=os.path.basename(__file__).replace(".py", ""),
        default_args={
            'retries': 1
        },
        description='A simple DAG to print information on terminal',
        start_date=datetime(2022, 6, 1),
        schedule=timedelta(seconds=60),
        catchup=False,
) as dag:

    task1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    task2 = BashOperator(
        task_id='print_text',
        depends_on_past=False,
        bash_command='echo HELLO',
        retries=3
    )

    def _my_func(**kwargs):
        print('current_datetime: {0}'.format(datetime.now()))

    task3 = PythonOperator(
        task_id='python_task',
        python_callable=_my_func
    )

    task1 >> task2 >> task3
