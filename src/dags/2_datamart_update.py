import logging 
import pendulum
from datetime import datetime, date, timedelta
from typing import List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.decorators import dag
from app_config.helper import AirflowHelper
import vertica_python


# initialize logger
logger = logging.getLogger(__name__)

# initialize airflow config object
airflow_variables = AirflowHelper()

def load_global_metrics_dm(sql_statement_path: str,
                           business_dt: date) -> None:
    pass
    vertica_conn = vertica_python.connect(
                                host = airflow_variables.vertica_host,
                                port = airflow_variables.vertica_port,
                                user = airflow_variables.vertica_user,
                                password = airflow_variables.vertica_password,
                                autocommit = 'True'
                                )
    # calculate T-1 day from airflow {{ds}}
    yesterday_dt = datetime.strptime(business_dt,"%Y-%m-%d").date() - timedelta(days=1) 

    logger.info(f"{business_dt= } and {yesterday_dt= }")

    sql_statement = open(sql_statement_path).read().format(processed_dt=yesterday_dt)

    with vertica_conn.cursor() as cur:
        cur.execute(sql_statement)

with DAG(
     dag_id='load_global_metrics', 
     schedule_interval="@daily", 
     start_date=pendulum.parse('2022-10-01'), 
     end_date=pendulum.parse('2022-11-01'), 
     catchup=True,
     tags=['final-project']
) as dag:
        
    load_cdm = PythonOperator(
                task_id="populate_datamart",
                python_callable=load_global_metrics_dm,
                op_kwargs = {"sql_statement_path": "/lessons/sql/dm_dwh_load.sql", "business_dt": "{{ds}}"},
                dag=dag
            )

    load_cdm