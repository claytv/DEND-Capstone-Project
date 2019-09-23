from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers.stage_redshift import StageToRedshiftOperator
from helpers.load_table import LoadTableOperator
from helpers.data_quality import DataQualityOperator
from helpers import sql_queries as sq



default_args = {
    'owner': 'claytv',
    'start_date': datetime(2019, 9, 8)
}

dag = DAG('ETLDag',
          default_args = default_args,
          description='Load and transform data in Redshift with Airflow',
        )




start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


staging_weather_to_redshift = StageToRedshiftOperator(
        task_id = 'staging_weather',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_weather',
        s3_path = 's3://claytv-dend-capstone/GlobalLandTemperaturesByCity.csv',
        copy_options = "DELIMITER',' IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD'"
)

staging_demographics_to_redshift = StageToRedshiftOperator(
        task_id='staging_us_demographics',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_us_demographics',
        s3_path='s3://claytv-dend-capstone/us-cities-demographics.csv',
        copy_options="DELIMITER ';' IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD'"
)
staging_flights_to_redshift = StageToRedshiftOperator(
        task_id='staging_flights',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_flights',
        s3_path='s3://claytv-dend-capstone/us_flight_data',
        copy_options="CSV IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD'"


)

load_cities_from_flights = LoadTableOperator(
        task_id='load_cities_from_flights',
        dag=dag,
        redshift_conn_id='redshift',
        table='cities',
        columns_sql='(city_name, state_code)',
        delete= True,
        sql=sq.CITIES_TABLE_INSERT_FLIGHTS
)


load_cities_from_demographics = LoadTableOperator(
    task_id='load_cities_from_demographics',
    dag=dag,
    redshift_conn_id='redshift',
    table='cities',
    columns_sql='(city_name, state_code)',
    delete=False,
    sql=sq.CITIES_TABLE_INSERT_DEMOGRAPHICS

)

load_demographics = LoadTableOperator(
    task_id ='load_demographics',
    dag=dag,
    redshift_conn_id='redshift',
    table='demographics',
    delete=True,
    sql=sq.DEMOGRAPHICS_TABLE_INSERT
)

load_time_from_flights = LoadTableOperator(
    task_id ='load_time_from_flights',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    delete=True,
    sql=sq.TIME_TABLE_INSERT_FLIGHTS
)

load_time_from_weather = LoadTableOperator(
    task_id='load_time_from_weather',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    delete=False,
    sql=sq.TIME_TABLE_INSERT_WEATHER
)

load_flights = LoadTableOperator(
    task_id='load_flights',
    dag=dag,
    redshift_conn_id='redshift',
    table='flights',
    columns_sql = '(date, origin_city_id, dest_city_id)',
    delete=True,
    sql=sq.FLIGHT_TABLE_INSERT
)

load_weather = LoadTableOperator(
    task_id='load_weather',
    dag=dag,
    redshift_conn_id='redshift',
    table='weather',
    delete=True,
    sql=sq.WEATHER_TABLE_INSERT
)
run_quality_checks = DataQualityOperator(
     task_id='Run_data_quality_checks',
     dag=dag,
     redshift_conn_id = 'redshift',
     tables = ["cities", "flights", "weather", "demographics", "time"]
 )
done_staging = DummyOperator(task_id= 'Done_staging', dag=dag)
done_load_fact = DummyOperator(task_id='Done_load_fact', dag=dag)
end_operator = DummyOperator(task_id= 'End_execution', dag=dag)


start_operator >> staging_weather_to_redshift
start_operator >> staging_demographics_to_redshift
start_operator >> staging_flights_to_redshift
staging_weather_to_redshift >> done_staging
staging_demographics_to_redshift >> done_staging
staging_flights_to_redshift >> done_staging
done_staging >> load_time_from_flights >> load_time_from_weather
done_staging >> load_cities_from_flights >> load_cities_from_demographics
load_time_from_weather >> done_load_fact
load_cities_from_demographics >> done_load_fact
done_load_fact >> load_demographics
done_load_fact >> load_flights
done_load_fact >> load_weather
load_demographics >> run_quality_checks
load_flights >> run_quality_checks
load_weather >> run_quality_checks
run_quality_checks >> end_operator














