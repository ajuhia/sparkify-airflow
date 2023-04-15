from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 3, 1),
    "depends_on_past": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

#Dag istantiation
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

#Task 1: Start the dag using a dummy operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Task 2: Copy log data from s3 to a staging table in redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id = 'redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    table='staging_events',
    json_param='s3://udacity-dend/log_json_path.json'
)

#Task 3: Copy songs data from s3 to a staging table in redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id = 'redshift',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    table='staging_songs',
    json_param='auto'
)

#Task 4: Create fact table from staging tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    sql_statement = SqlQueries.songplay_table_insert
)

#Task 5: load users dimension table 
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql_statement = SqlQueries.user_table_insert,
    append_row=False
)

#Task 6: load songs dimension table 
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    sql_statement = SqlQueries.song_table_insert,
    append_row=False
)

#Task 7: load artists dimension table 
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    sql_statement = SqlQueries.artist_table_insert,
    append_row=False
)

#Task 8: load time dimension table 
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql_statement = SqlQueries.time_table_insert,
    append_row=False
)

#Task 9: Run data quality checks on fact and dimension tables 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ['songplays', 'users', 'time', 'artists', 'songs']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#Task execution flow
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator