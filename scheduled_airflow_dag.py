import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def basic_dag_tester():
    '''
    Test Basic functionality of DAGs with airflow
    '''
    logging.info('basic DAG test')


dag = DAG(
        'lesson1.exercise2',
        start_date=datetime.datetime.now() - datetime.timedelta(days=15),
        schedule_interval='@daily')


dag_test_task = PythonOperator(
    task_id="dag_tester_with_schedule",
    python_callable=basic_dag_tester,
    dag=dag
)