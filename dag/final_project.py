from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.data_quality import DataQualityOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from sql.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('final_project_dag',
          default_args=default_args,
          start_date=datetime.now(),
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=2
          )

script_dir = os.path.dirname(__file__)  # <-- absolute dir the script is in
rel_path = "create_tables.sql"
abs_file_path = os.path.join(script_dir, rel_path)

create_tables_sql = None
with open(abs_file_path) as file:
    create_tables_sql = file.read()

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sascha-redshift-bucket",
    s3_key="log-data",
    json_path="s3://sascha-redshift-bucket/log_json_path.json"

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sascha-redshift-bucket",
    s3_key="song-data",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "artists", "time", "songs", "users"]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [
    load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
    load_time_dimension_table] >> run_quality_checks >> end_operator