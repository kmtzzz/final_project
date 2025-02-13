from airflow.models import Variable

class AirflowHelper:

    def __init__(self):
        
        # set credentials from Airflow variables for S3 connection
        self.s3_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
        self.s3_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
        self.s3_service_name = 's3'
        self.s3_endpoint_url = Variable.get("S3_ENDPOINT_URL")
        self.s3_bucket = Variable.get("S3_BUCKET")

        # set credentials from Airflow variables for Vertica connection
        self.vertica_host = Variable.get('VERTICA_HOST')
        self.vertica_port = Variable.get('VERTICA_PORT')
        self.vertica_user = Variable.get('VERTICA_USER')
        self.vertica_password = Variable.get('VERTICA_PASSWORD')
