from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials,
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.tables = tables

    def execute(self, context):
        
        redshift =  PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Working on getting the quality check done..')
        for table in self.tables:
            check = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            if check[0][0] > 1 or check[0][0] > 0:
                self.log.info(f'{table} is looking good!')
            else: 
                raise ValueError(f'{table} did not pass the quality check...')
                
        self.log.info('Quality check is done.')