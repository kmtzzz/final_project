import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
import boto3
import boto3.session

# Read credentials from Airflow variables
access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

# move session creation outside function
session = boto3.session.Session()

def fetch_s3_file(bucket: str, key: str) -> str:

    s3_client = session.client(
                                service_name='s3',
                                endpoint_url='https://storage.yandexcloud.net',
                                aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_access_key,
                            )
    s3_client.download_file(
                            Bucket=bucket,
                            Key=key,
                            Filename=f'/data/{key}'
                            )