from datetime import datetime, timedelta
import os
import logging 
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator, StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries 

default_args = {
    'owner': 'architetto', 
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False, 
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'catchup_by_default': False 
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

def create_tables():

    redshift = PostgresHook(postgres_conn_id= 'redshift')
    
    for table in SqlQueries.create_tables:
        redshift.run(table)

create_redshift_tables = PythonOperator(
    task_id = 'create_redshift_tables',
    python_callable = create_tables,
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    table='staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    file_type = 'json',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=dag,
    table='staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    file_type = 'json',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    select_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    select_query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    select_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    select_query = SqlQueries.artist_table_insert 
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    select_query = SqlQueries.time_table_insert 
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    tables = ['artists', 'songplays', 'songs', 'time', 'users']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 

load_songplays_table  >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table] >> run_quality_checks 

run_quality_checks >> end_operator 


