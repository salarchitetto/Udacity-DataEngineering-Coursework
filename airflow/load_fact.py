from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    sql_insert = """ 
    INSERT INTO {}
    {};
    COMMIT;
    """
    
    @apply_defaults
    def __init__(self,

                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
#         self.aws_credentials_id = aws_credentials_id,
        self.table = table,
        self.query = query,

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Loading information into the fact table')
        formatted_sql = LoadFactOperator.sql_insert.format(
            self.table,
            self.query
        )
        self.log.info(formatted_sql)
        redshift.run(str(formatted_sql))
        
