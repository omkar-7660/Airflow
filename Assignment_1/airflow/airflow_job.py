from airflow import DAG
from airflow.models import Variable

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args={
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    }

with DAG(

    dag_id='moving_data_from_GCS_to_HIVE',
    default_args=default_args,
    description='A DAG to move JSON data from GCS to Hive using Dataproc',
    schedule_interval=None,
    catchup=False,
    tags=['dataproc', 'gcs', 'hive'],
    max_active_runs=1,  # Limit to one active run at a time


) as dag :
    
    #Required parameters for the cluster or job
    PROJECT_ID = Variable.get('PROJECT_ID')
    REGION = Variable.get('REGION')
    CLUSTER_NAME = Variable.get('CLUSTER_NAME')
    CLUSTER_CONFIG = Variable.get('CLUSTER_CONFIG', deserialize_json=True)
    GCS_BUCKET = Variable.get('gcs_bucket')


    Scan_file_arrival=GCSObjectExistenceSensor(
        task_id='check_gcs_file',
        bucket=GCS_BUCKET,
        object=f"Assignment_1/source_prod/employee.csv",
        timeout=300,
        poke_interval=30,
        mode='poke',        
    )

    Create_cluster=DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
    )

    pyspark_job={
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'gs://{GCS_BUCKET}/Assignment_1/spark_job/spark_hive_job.py',
    }
    }
    Submit_job=DataprocSubmitJobOperator(
        task_id='submit_hive_job',
        project_id=PROJECT_ID,
        region=REGION,
        job=pyspark_job,
    )
    Delete_cluster=DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule='all_done',  # Ensure cluster deletion happens even if job fails
    )

    Scan_file_arrival >> Create_cluster >> Submit_job >> Delete_cluster

