from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_statement = """
            COPY {}
            FROM '{}' 
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION 'us-west-2'
            JSON '{}';
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_key="",
                 file_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_key = s3_key
        self.file_format = file_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credential = aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clear table from Redshift")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copy data from Udacity S3 to Redshift")
        s3_path = "s3://udacity-dend/{}".format(self.s3_key)
        
        formatted_sql = StageToRedshiftOperator.copy_statement.format(
            self.table,
            s3_path,
            credential.access_key,
            credential.secret_key,
            self.file_format
        )
        
        redshift.run(formatted_sql)
        
        self.log.info(f'Successfull copy data to {self.table} from S3')






