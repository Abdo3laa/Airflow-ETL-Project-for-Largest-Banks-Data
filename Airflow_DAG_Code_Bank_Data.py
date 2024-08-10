from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import sqlite3
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as soup
from io import StringIO

# File and URL paths
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = '/opt/airflow/dags/largest_banks.csv'  # Adjusted path
log_path = '/opt/airflow/dags/etl_project_log.txt'  # Adjusted path
exchange_rate_path = '/opt/airflow/dags/exchange_rate.csv'  # Adjusted path
db_name = 'Banks.db'
table_name = 'Largest_banks'

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 8),  # Set to a past date or today
    'retries': 1,
}

# Define the DAG
with DAG(
    'bank_data_dag',
    default_args=default_args,
    schedule_interval=None,  # No automatic schedule
    catchup=False,          # Do not backfill
    tags=['manual']         # Optional: tag for easy identification
) as dag:

    def log_progress(message, log_path):
        timestamp_format = '%Y-%b-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a") as f:
            f.write(timestamp + ' : ' + message + '\n')

    def extract(url):
        page = requests.get(url).text
        data = soup(page, 'html.parser')
        heading = data.find('span', {'id': 'By_market_capitalization'})
        table = heading.find_next('table', {'class': 'wikitable'})
        df = pd.read_html(StringIO(str(table)))[0]
        df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].apply(lambda x: float(str(x).strip()[:-1]))
        return df

    def transform(df, exchange_rate_path):
        exchange_rate = pd.read_csv(exchange_rate_path, encoding='utf-8')  # Explicit encoding
        df['MC_GBP_Billion'] = np.round(df['Market cap (US$ billion)'] * exchange_rate['GBP'].iloc[0], 2)
        df['MC_EUR_Billion'] = np.round(df['Market cap (US$ billion)'] * exchange_rate['EUR'].iloc[0], 2)
        df['MC_INR_Billion'] = np.round(df['Market cap (US$ billion)'] * exchange_rate['INR'].iloc[0], 2)
        return df

    def load_to_csv(df, csv_path):
        df.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")

    def load_to_db(conn, table_name, df):
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data loaded into table {table_name} in database.")

    log_task = PythonOperator(
        task_id='log_progress',
        python_callable=log_progress,
        op_args=['ETL process started', log_path],
        dag=dag
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_args=[url],
        dag=dag
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        op_args=[extract_task.output, exchange_rate_path],
        dag=dag
    )

    load_csv_task = PythonOperator(
        task_id='load_to_csv',
        python_callable=load_to_csv,
        op_args=[transform_task.output, csv_path],
        dag=dag
    )

    load_db_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        op_args=[sqlite3.connect(db_name), table_name, transform_task.output],
        dag=dag
    )

    log_task >> extract_task >> transform_task >> [load_csv_task, load_db_task]