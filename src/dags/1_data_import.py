import pendulum
import contextlib
import vertica_python
import os
import pandas as pd
from datetime import datetime, date
from typing import List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import boto3.session
from app_config.helper import AirflowHelper
import logging 


# initialize logger
logger = logging.getLogger(__name__)

# initialize airflow config object
airflow_variables = AirflowHelper()
file_root_path = '/data'

logger.info(f"Starting process")
# move session creation outside function
session = boto3.session.Session()

s3_client = session.client(
                            service_name = airflow_variables.s3_service_name,
                            endpoint_url = airflow_variables.s3_endpoint_url,
                            aws_access_key_id = airflow_variables.s3_access_key_id,
                            aws_secret_access_key = airflow_variables.s3_secret_access_key,
                        )


def fetch_s3_files(bucket: str, path: str) -> None:
    logger.info(f"Starting fetching S3")

    try:
        s3_file_list = s3_client.list_objects_v2(Bucket = bucket)

        for file in s3_file_list['Contents']:
            s3_client.download_file(
                                    Bucket = bucket,
                                    Key = file['Key'],
                                    Filename = f"{path}/{file['Key']}"
                                )
            logger.info(f"File {file['Key']} is downloaded")

        # merge all transaction files into one
        files = [f for f in os.listdir(path) if 'transactions_batch' in f]
        transactions_all = pd.DataFrame()
        for file in files:
            data = pd.read_csv(f"{path}/{file}")
            transactions_all = pd.concat([transactions_all, data])
        
        transactions_all.to_csv(f"{path}/transactions_all.csv", index=False)
        logger.info(f"File transactions_all.csv is created")
    except Exception as e:
        print(f"Exception {e} during S3 download occured")

def load_to_vertica(path: str, 
                    dataset_file: str, 
                    schema: str, 
                    table: str, 
                    columns_list: List[str], 
                    business_dt: date) -> None:
    
    # fetch file into dataframe
    file_df = pd.read_csv(f"{path}/{dataset_file}")

    # filter dataframe by date
    date_column = 'transaction_dt' if table=='transactions' else 'date_update'
    file_df[date_column] = pd.to_datetime(file_df[date_column])
    processing_df = file_df.loc[file_df[date_column].dt.date == datetime.strptime(business_dt,"%Y-%m-%d").date()]

    # Proceed with load to Vertica
    num_rows = len(processing_df) # number of records in final dataset
    logger.info(f"{num_rows= }")
    logger.info(f"{business_dt= }")

    wildcards = ["%s"] * len(processing_df.columns)

    delete_expr = f"DELETE FROM {schema}.{table} WHERE {date_column}::date = '{business_dt}'::date"
    insert_expr = f"INSERT INTO {schema}.{table} ({', '.join(processing_df.columns)}) VALUES ({', '.join(wildcards)})"

    chunk_size = num_rows // 100 if num_rows > 50000 else num_rows
    logger.info(f"{chunk_size= }")

    if num_rows>0:
        vertica_conn = vertica_python.connect(
                                host = airflow_variables.vertica_host,
                                port = airflow_variables.vertica_port,
                                user = airflow_variables.vertica_user,
                                password = airflow_variables.vertica_password,
                                autocommit = 'True'
                                )
        with vertica_conn.cursor() as cur:

            cur.execute(delete_expr) # idempotecy support
            
            cur.executemany(insert_expr, list(zip(*map(processing_df.get, processing_df))))


with DAG(
     dag_id='S3_to_Staging', 
     schedule_interval="@daily", 
     start_date=pendulum.parse('2022-10-01'), 
     end_date=pendulum.parse('2022-10-31'), 
     catchup=True,
     tags=['final-project']
) as dag:
    
    upload_data_from_s3 = PythonOperator(
                task_id="fetch_data_from_s3",
                python_callable=fetch_s3_files,
                op_kwargs = {"bucket": airflow_variables.s3_bucket, "path": file_root_path},
                dag=dag
            )
    
    load_transactions = PythonOperator(
                task_id="load_transactions_to_vertica",
                python_callable=load_to_vertica,
                op_kwargs = {"path": file_root_path,
                             "dataset_file": "transactions_all.csv",
                             "schema": "stv2024080626__staging",
                             "table": "transactions",
                             "columns_list": ["operation_id", "account_number_from", "account_number_to", "currency_code", "country", "status", "transaction_type", "amount", "transaction_dt"],
                             "business_dt": "{{ds}}"
                             },
                dag=dag
            )

    load_currencies = PythonOperator(
                task_id="load_currencies_to_vertica",
                python_callable=load_to_vertica,
                op_kwargs = {"path": file_root_path,
                             "dataset_file": "currencies_history.csv",
                             "schema": "stv2024080626__staging",
                             "table": "currencies",
                             "columns_list": ["date_update", "currency_code", "currency_code_with", "currency_with_div"],
                             "business_dt": "{{ds}}"
                             },
                dag=dag
            )   

    upload_data_from_s3 >> load_currencies >> load_transactions