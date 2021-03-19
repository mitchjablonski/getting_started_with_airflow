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
        'lesson1.exercise1',
        start_date=datetime.datetime.now())


dag_test_task = PythonOperator(
    task_id="dag_tester",
    python_callable=basic_dag_tester,
    dag=dag
)