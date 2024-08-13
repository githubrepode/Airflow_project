from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from sqlalchemy import create_engine

import pandas as pd
import requests
import logging
import os
from datetime import timedelta

# Set up logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_data(**kwargs):
    url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv"
    output_path = '/tmp/covid_data.csv'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Successfully downloaded data to {output_path}")
    except requests.RequestException as e:
        logger.error(f"Error downloading data: {e}")
        raise AirflowException(f"Data download failed: {e}")

def transform_data(**kwargs):
    input_path = '/tmp/covid_data.csv'
    output_path = '/tmp/covid_data_transformed.csv'
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    try:
        df = pd.read_csv(input_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df_transformed = df.groupby(['Date', 'Country'])['Confirmed', 'Recovered', 'Deaths'].sum().reset_index()
        df_transformed.to_csv(output_path, index=False)
        
        logger.info(f"Successfully transformed data and saved to {output_path}")
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise AirflowException(f"Data transformation failed: {e}")


def load_data(**kwargs):
    input_path = '/tmp/covid_data_transformed.csv'
    table_name = 'covid_data'
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection_uri = postgres_hook.get_uri()
        engine = create_engine(connection_uri)
        
        df = pd.read_csv(input_path)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = result.scalar()
        
        logger.info(f"Successfully loaded {row_count} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise AirflowException(f"Data loading failed: {e}")

with DAG(
    'sample_etl_pipeline',
    default_args=default_args,
    description='A sample ETL pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['sample'],
) as dag:

    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    create_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS covid_data (
            Date DATE,
            Country TEXT,
            Confirmed INTEGER,
            Recovered INTEGER,
            Deaths INTEGER
        )
        """
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    download_task >> transform_task >> create_table_task >> load_task