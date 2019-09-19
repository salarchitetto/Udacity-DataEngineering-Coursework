from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    #keeping this here for now until project is completed.
#     dimension_query = """ 
#         INSERT INTO {table}
#         {select_query}
#     """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 select_query,
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'uploading information to {self.table}...')
        self.log.info(self.select_query)
        
        if self.append_data == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.select_query)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table_name
            redshift.run(sql_statement)

            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.select_query)
            redshift.run(sql_statement)
        
        #keeping this bit here for now until projected is officially completed. 
#         redshift.run(LoadDimensionOperator.dimension_query.format(
#             table=self.table,
#             select_query=self.select_query
#         ))
        
        self.log.info(f'finished uploading {self.table} table!')
        
