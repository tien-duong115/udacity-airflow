from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
   
    payload = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        {}
        ;
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 table_name="",
                 region="",
                 use_json="",  
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id='aws_credentials'
        self.table_name=table_name
        self.s3_bucket=s3_bucket
        self.s3_key = s3_key
        self.region=region
        self.use_json=use_json

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Copying data from S3 to Redshift staging {self.table_name} table")
        rendered_key = self.s3_key.format(**context)
        self.log.info(f"Rendered Key: {rendered_key}")
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        posting_payload = StageToRedshiftOperator.payload.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.use_json
        )
        self.log.info(f"Executing query to copy data from '{s3_path}' to '{self.table_name} with posting_payload:{posting_payload}'")
        redshift.run(posting_payload)