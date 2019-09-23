from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import sql_queries as sq

default_args = {
    'owner': 'claytv',
    'start_date': datetime(2019, 9, 8),
    'retries':0
}

dag = DAG('create_table_task',
          default_args=default_args,
          description="Drop and create tables that will be loaded in Redshift"
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

drop_table_staging_weather = PostgresOperator(
    task_id="drop_staging_weather",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_STAGING_WEATHER
)


drop_table_staging_us_demographics = PostgresOperator(
    task_id="drop_staging_us_demographics",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_STAGING_US_DEMOGRAPHICS
)

drop_table_staging_flights = PostgresOperator(
    task_id="drop_staging_flights",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_STAGING_FLIGHTS
)
drop_table_flights = PostgresOperator(
    task_id="drop_flights",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_FLIGHTS
)
drop_table_cities = PostgresOperator(
    task_id="drop_cities",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_CITIES
)

drop_table_weather = PostgresOperator(
    task_id="drop_weather",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_WEATHER
)
drop_table_demographics = PostgresOperator(
    task_id="drop_demographics",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_DEMOGRAPHICS
)
drop_table_time = PostgresOperator(
    task_id="drop_time",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.DROP_TIME
)
create_table_staging_weather = PostgresOperator(
    task_id="create_staging_weather",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_STAGING_WEATHER
)


create_table_staging_us_demographics = PostgresOperator(
    task_id="create_staging_us_demographics",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_STAGING_US_DEMOGRAPHICS
)

create_table_staging_flights = PostgresOperator(
    task_id="create_staging_flights",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_STAGING_FLIGHTS
)

create_table_flights = PostgresOperator(
    task_id="create_flights",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_FLIGHTS
)
create_table_cities = PostgresOperator(
    task_id="create_cities",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_CITIES
)

create_table_weather = PostgresOperator(
    task_id="create_weather",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_WEATHER
)

create_table_demographics = PostgresOperator(
    task_id="create_demographics",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_DEMOGRAPHICS
)

create_table_time = PostgresOperator(
    task_id="create_time",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sq.CREATE_TIME
)





end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_table_staging_weather
start_operator >> drop_table_staging_us_demographics
start_operator >> drop_table_staging_flights
start_operator >> drop_table_flights
start_operator >> drop_table_cities
start_operator >> drop_table_demographics
start_operator >> drop_table_time
start_operator >> drop_table_weather
drop_table_staging_weather >> create_table_staging_weather
drop_table_staging_flights >> create_table_staging_flights
drop_table_staging_us_demographics >> create_table_staging_us_demographics
drop_table_flights >> create_table_flights
drop_table_cities >> create_table_cities
drop_table_weather >> create_table_weather
drop_table_demographics >> create_table_demographics
drop_table_time >> create_table_time
create_table_staging_weather >> end_operator
create_table_staging_flights >> end_operator
create_table_staging_us_demographics >> end_operator
create_table_flights >> end_operator
create_table_cities >> end_operator
create_table_weather >> end_operator
create_table_demographics >> end_operator
create_table_time >> end_operator

