from datetime import datetime, timedelta
import os
from airflow import DAG
from ods.homebrew_fivetran.udfs import homebrew_fivetran, snowflake_migrations
from utils.others.common_functions import on_failure_callback
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    "owner": "david@tresl.co",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}
dag = DAG(
    "HOMEBREW_FIVETRAN",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */6 * * *",
    max_active_runs=1,
    catchup=False,
)

with dag:
    EXPORT_2_S3 = PythonOperator(
        task_id="EXPORT_2_S3", python_callable=homebrew_fivetran.main, dag=dag,
        priority_weight=os.getenv('HIGH_PRIORITY')
    )
    S3_2_SNOWFLAKE = PythonOperator(
        task_id="S3_2_SNOWFLAKE",
        python_callable=snowflake_migrations.main,
        provide_context=True,
        dag=dag,
        priority_weight=os.getenv('HIGH_PRIORITY')
    )
    EXPORT_2_S3 >> S3_2_SNOWFLAKE
