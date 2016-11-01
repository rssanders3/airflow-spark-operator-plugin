from __future__ import print_function
from airflow.operators import SparkSubmitOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import os


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

APPLICATION_FILE_PATH = "~/spark-test/spark_test.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    }

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None, start_date=(datetime.now() - timedelta(minutes=1)))

dummy = SparkSubmitOperator(
    task_id='spark-submit-python',
    application_file=APPLICATION_FILE_PATH,
    application_args="arg1 arg2",
    dag=dag)

