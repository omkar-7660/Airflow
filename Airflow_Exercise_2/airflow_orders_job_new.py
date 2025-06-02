from airflow import  DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'start_date':datetime(2025,6,1),

}
dag = DAG(
    'orders_backfilling_dag',
    default_args=default_args,
    description='A DAG to run Spark job with input parameter on Dataproc',
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    params={
        'execution_date':Param(
            default='NA',
            type='string',
            description='Execution date in yyyymmdd format'
        )
    }

)


def get_execution_date(ds_nodash,**kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

exe_python_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ds_nodash}}'},
    dag=dag,
)


exe_bash_task= BashOperator(
    task_id='print_date',
    bash_command='echo "Derived execution date: {{ ti.xcom_pull(task_ids=\'get_execution_date\') }}" &&'
    'echo "orders_{{ ti.xcom_pull(task_ids=\'get_execution_date\') }}.csv will be processed"',
    dag=dag,
)

# Fetch configuration from Airflow variables

basic_details = Variable.get('CLUSTER_BASIC',deserialize_json=True)
CLUSTER_NAME=basic_details['CLUSTER_NAME'] 
PROJECT_ID=basic_details['PROJECT_ID']
REGION = basic_details['REGION']


CLUSTER_CONFIG = Variable.get('CLUSTER_CONFIG',deserialize_json=True)

create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag
)


# Submit a PySpark job to the Dataproc cluster
PYSPARK_JOB = {
 "reference": {"project_id": PROJECT_ID},
 "placement": {"cluster_name": CLUSTER_NAME},
 "pyspark_job": {
    "main_python_file_uri": "gs://us-central1-airflow-dev-bcf3e1dc-bucket/jobs/orders_data_process.py",
    "args": ["--date", "{{ ti.xcom_pull(task_ids='get_execution_date') }}"],  
    # Passing date as an argument to the PySpark script

 },
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job_on_dataproc',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Delete the Dataproc cluster
delete_cluster=DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule='all_done',  # Ensure this runs even if the create task fails
    dag=dag,
)


exe_python_task >> [exe_bash_task,create_cluster] >> submit_pyspark_job >> delete_cluster