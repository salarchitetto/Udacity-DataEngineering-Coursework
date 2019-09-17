from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging 


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    #copy command for songs data from s3 to redshift
    
    copy_songs_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT AS 'epochmillisecs'
        REGION 'us-west-2'
        COMPUPDATE OFF 
        FORMAT AS JSON 'auto'
    """
    
    copy_events_json = """ 
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        REGION 'us-west-2'
        COMPUPDATE OFF 
        FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 create_sql_table = '',
                 s3_bucket = '',
                 s3_key = '',
                 region = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.create_sql_table = create_sql_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        self.log.info('Connecting to AWS and Redshift VIA creds')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.table == 'staging_songs':
            formatted_sql = StageToRedshiftOperator.copy_songs_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
            self.log.info(formatted_sql)
            
        else:
            formatted_sql = StageToRedshiftOperator.copy_events_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )

        redshift.run(formatted_sql)
        self.log.info('Loaded into staging table properly! ')
       
