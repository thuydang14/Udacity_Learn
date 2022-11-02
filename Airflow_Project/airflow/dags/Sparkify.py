from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'thuydd7',
    'start_date': datetime(2019, 1, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs': 3,
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup_by_default': False
}

dag = DAG('thuy_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_events",
    s3_key="log_data/",
    file_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_songs",
    s3_key="song_data",
    file_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql_query=SqlQueries.user_table_insert,
    truncate="True"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    sql_query=SqlQueries.song_table_insert,
    truncate="True"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    sql_query=SqlQueries.artist_table_insert,
    truncate="True"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql_query=SqlQueries.time_table_insert,
    truncate="True"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    param = [
        {
            'sql': 'SELECT COUNT(*) FROM {};',
            'op': 'gt',
            'val': 0,
            'tables': ['songplays','users','artists','songs','time']
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_time_dimension_table, load_artist_dimension_table, load_song_dimension_table, load_user_dimension_table] >> run_quality_checks >> end_operator



