
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadFactOperator, StageToRedshiftOperator, LoadDimensionOperator, DataQualityOperator, PostgresOperator
from helpers import SqlQueries

default_args = {
    'owner': 'TienDuong',
    'start_date': datetime.now(),
    'end_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once',
          max_active_runs=1,
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name="staging_events",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    use_json="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name="staging_songs",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="songs_data",
    region="us-west-2",
    use_json="JSON 'auto' COMPUPDATE OFF"
    )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name="songplays",
    redshift_conn_id="redshift",
    pay_load= SqlQueries.songplays_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name='users',
    redshift_conn_id='redshift',
    pay_load=SqlQueries.users_table_insert
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name='songs',
    redshift_conn_id='redshift',
    pay_load=SqlQueries.songs_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name='artists',
    redshift_conn_id='redshift',
    pay_load=SqlQueries.artists_table_insert
    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name='time',
    redshift_conn_id='redshift',
    pay_load=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    sql='SELECT COUNT(*) FROM time',
    expected_result=0,
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table  >> run_quality_checks
load_user_dimension_table  >> run_quality_checks

run_quality_checks >> end_operator
