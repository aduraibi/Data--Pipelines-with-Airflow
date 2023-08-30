from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
import final_project_operators.sql_queries as sql_queries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner':'udacity',
    'depends_on_past': False, #The DAG does not have dependencies on past runs
    'start_date': datetime(2018,11,1),
    # 'start_date': datetime.now(),
    'end_date': datetime(2018,11,30),
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'catchup': False, #Catchup is turned off
    'email_on_retry': False #Do not email on retry
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly' #The DAG should be scheduled to run once an hour
)
def final_project():

    
    start_operator = DummyOperator(task_id='Begin_execution')
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table= 'staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='sparkify-songs',
        s3_key='s3://sparkify-songs/log-data',
        region='us-east-1',
        json_format = 's3://sparkify-songs/log-data/log_json_path.json',
        sql_create_statement = sql_queries.staging_events_table_create
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table= 'staging_songs',
        redshift_conn_id= 'redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket= 'udacity-dend',
        s3_key='s3://udacity-dend/song_data',
        region='us-west-2',
        json_format = 'auto' ,
        sql_create_statement = sql_queries.staging_songs_table_create
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        redshift_conn_id='redshift',
        sql_create_statement = sql_queries.songplays_table_create,
        sql_load_statement = sql_queries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        redshift_conn_id='redshift',
        sql_create_statement = sql_queries.user_table_create,
        sql_load_statement = sql_queries.user_table_insert,
        truncate_insert = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        redshift_conn_id='redshift',
        sql_create_statement =sql_queries.songs_table_create,
        sql_load_statement = sql_queries.song_table_insert,
        truncate_insert = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        redshift_conn_id='redshift',
        sql_create_statement=sql_queries.artist_table_create,
        sql_load_statement= sql_queries.artist_table_insert,
        truncate_insert = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        redshift_conn_id='redshift',
        sql_create_statement = sql_queries.time_table_create,
        sql_load_statement = sql_queries.time_table_insert,
        truncate_insert = False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        checks= [
            {'test_sql': "SELECT COUNT(*) FROM songplays" , 'expected_result': 0 , 'comparison': '>'}
        ],
        redshift_conn_id='redshift',
    )

   
    end_operator = DummyOperator(task_id='Stop_execution')
    
    start_operator >> [stage_events_to_redshift , stage_songs_to_redshift] 
    
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table  >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table ] >> run_quality_checks >> end_operator

final_project_dag = final_project()