from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ===============================================
# DAG CONFIGURATION
# ===============================================

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='parent_rental_ingest',
    default_args=default_args,
    description='Ingestion Pipeline to HDFS',
    schedule_interval=None,
    catchup=False,
    tags=['ingest', 'hdfs', 'parent'],
) as dag:

    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )

    ingest_to_hdfs = BashOperator(
        task_id='ingest_to_hdfs',
        bash_command="bash -c '/home/hadoop/scripts/rental.sh'",
    )

    trigger_child_transform = TriggerDagRunOperator(
        task_id="trigger_child_transform",
        trigger_dag_id="child_rental_spark",
        conf={
            "execution_user": "airflow",
            "execution_time": "{{ ds }}",
        }
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )
# ===============================================
#            TASK DEPENDENCIES
# ===============================================
start_pipeline >> ingest_to_hdfs >> trigger_child_transform >> end_pipeline
