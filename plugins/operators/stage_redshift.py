from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from pathlib import Path


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                FORMAT AS JSON '{}' ;
                """
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id = '',
                 redshift_conn_id = '',
                 s3_bucket = '',
                 s3_key = '',
                 table = '',
                 json_param='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.json_param = json_param
        
    def execute(self, context):
        """
        This function:
        - fetches all values from the hooks.
        - creates all the required staging and actual tables if not exists.
        - deletes existing data from the desired staging table.
        - inserts data into the desired staging table by formatting and executing copy_sql defined above.
        """
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        
        self.log.info("Create all the staging and dimension tables if not exists!")
        create_sql = Path(Path(__file__).parent.parent.parent/"create_tables.sql").read_text()
        redshift.run(create_sql)
        
        self.log.info("Clear previous data from  Redshift table!")
        redshift.run(f"DELETE FROM {self.table} ")
        
        self.log.info("Copy data from S3 to the desired staging redshift table!")
        rendered_key = self.s3_key.format()
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_param)
        
        redshift.run(formatted_sql)
        
        
        self.log.info(f"Data insertion completed for {self.table} table!")





