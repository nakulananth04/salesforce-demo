from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import boto3
import time
import requests
import json

AWS_REGION = 'ap-south-1'
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'email': ['nakulananths@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    'salesforce_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['salesforce', 'etl', 'dbt']
)

# ----------------------------------------
# Slack Callbacks
# ----------------------------------------
def slack_failure_alert(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = context.get('task').task_id
    execution_date = context.get('ts')
    log_url = context.get('task_instance').log_url

    message = {
        'text': f"? *Task Failed*: `{dag_id}.{task_id}`\n*Execution Time*: {execution_date}\n?? <{log_url}|View Logs>"
    }

    requests.post(SLACK_WEBHOOK_URL, data=json.dumps(message))


def slack_dag_success_alert():
    message = {
        'text': "? *DAG Succeeded*: `salesforce_pipeline`\nAll tasks completed successfully."
    }
    requests.post(SLACK_WEBHOOK_URL, data=json.dumps(message))

notify_success = PythonOperator(
    task_id='notify_dag_success',
    python_callable=slack_dag_success_alert,
    trigger_rule='all_success',
    dag=dag
)

# ----------------------------------------
# AWS Lambda invocations
# ----------------------------------------
def invoke_lambda(function_name):
    client = boto3.client('lambda', region_name=AWS_REGION)
    client.invoke(FunctionName=function_name, InvocationType='RequestResponse')

run_lambda_1 = PythonOperator(
    task_id='invoke_salesforce_marketing_lambda',
    python_callable=lambda: invoke_lambda('SalesforceMarketingDataGenLambda'),
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

run_lambda_2 = PythonOperator(
    task_id='invoke_migrate_s3_to_snowflake_lambda',
    python_callable=lambda: invoke_lambda('MigrateS3ToSnowflakeLambda'),
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

# ----------------------------------------
# Glue Jobs
# ----------------------------------------
def run_glue_job(job_name):
    client = boto3.client('glue', region_name=AWS_REGION)
    response = client.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']

    while True:
        status = client.get_job_run(JobName=job_name, RunId=job_run_id)
        state = status['JobRun']['JobRunState']
        if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(30)

    if state != 'SUCCEEDED':
        raise Exception(f"Glue job '{job_name}' failed with state: {state}")

run_glue_cleaner = PythonOperator(
    task_id='run_parquet_cleaner_glue_job',
    python_callable=lambda: run_glue_job('parquet-cleaner-job'),
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

run_glue_incremental = PythonOperator(
    task_id='run_iceberg_incremental_glue_job',
    python_callable=lambda: run_glue_job('salesforce-iceberg-incremental'),
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

# ----------------------------------------
# DBT Tasks
# ----------------------------------------
dbt_env_activate = 'cd /home/ubuntu/salesforce_datapipeline && source dbt_venv/bin/activate && cd ../salesforce_marketing'

run_dbt_staging = BashOperator(
    task_id='dbt_run_staging_transformations',
    bash_command=f'{dbt_env_activate} && dbt run --select staging_transformations',
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

run_dbt_dimensions = BashOperator(
    task_id='dbt_run_dimensions',
    bash_command=f'{dbt_env_activate} && dbt run --select dimensions',
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

run_dbt_facts = BashOperator(
    task_id='dbt_run_facts',
    bash_command=f'{dbt_env_activate} && dbt run --select facts',
    on_failure_callback=slack_failure_alert,
    dag=dag,
)

# ----------------------------------------
# DAG Dependency Chain
# ----------------------------------------
run_lambda_1 >> run_glue_cleaner >> run_glue_incremental >> run_lambda_2
run_lambda_2 >> run_dbt_staging >> run_dbt_dimensions >> run_dbt_facts >> notify_success
