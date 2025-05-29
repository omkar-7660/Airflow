from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

#default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date':days_ago(1)
}



dag=DAG(
    'dataProc_cluster_setup_new',
    default_args=default_args,
    description='A DAG to setup and delete a Dataproc cluster',
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
    tags=['dev'],

)

# Define cluster configuration
PROJECT_ID = "halogen-oxide-459605-b6"
REGION = "us-central1"
CLUSTER_NAME = "dataproc-cluster-demo"

CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.2.26-debian12'
    }
}


# Create the Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    cluster_config=CLUSTER_CONFIG,
    dag=dag
)


# Submit a PySpark job to the Dataproc cluster
PYSPARK_JOB = {
 "reference": {"project_id": PROJECT_ID},
 "placement": {"cluster_name": CLUSTER_NAME},
 "pyspark_job": {
 "main_python_file_uri": "gs://us-central1-learning-compos-dac16dfc-bucket/jobs/wordcount.py",
 },
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job_on_dataproc',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

# Delete the Dataproc cluster
delete_cluster=DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule='all_done',  # Ensure this runs even if the create task fails
    dag=dag
)


# Set task dependencies
create_cluster >> submit_pyspark_job >> delete_cluster