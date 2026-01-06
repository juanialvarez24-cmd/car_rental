from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

# ===============================================
#             DAG CONFIGURATION
# ===============================================

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='child_rental_spark',
    default_args=default_args,
    description='Transformation and Loading Pipeline: Spark to Hive',
    schedule_interval=None,
    catchup=False,
    tags=['transformation', 'hive', 'child'],
) as dag:

    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )

    load_to_hive = BashOperator(
        task_id='load_to_hive',
        bash_command="""
            ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit \
            --files /home/hadoop/hive/conf/hive-site.xml \
            /home/hadoop/scripts/rental_load.py
        """,
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )
  
# ===============================================
#              TASK DEPENDENCIES
# ===============================================
start_pipeline >> load_to_hive >> end_pipeline
