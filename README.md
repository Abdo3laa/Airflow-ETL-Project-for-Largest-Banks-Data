# ETL Pipeline for Bank Data
## Project Overview
Welcome to the ETL pipeline project for processing bank data. This pipeline is designed to extract data from a web source, transform it with exchange rates, and load the results into both a CSV file and an SQLite database. We utilize Apache Airflow to orchestrate the ETL process, ensuring smooth and efficient execution.

## File and URL Paths
### Here’s where you’ll find the essential files and URLs for this project:

- Data Source URL: `https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks`
- Path to Save CSV: `/opt/airflow/dags/largest_banks.csv`
- Path to Log File: `/opt/airflow/dags/etl_project_log.txt`
- Exchange Rate CSV Path: `/opt/airflow/dags/exchange_rate.csv`
- SQLite Database Name: `Banks.db`
- SQLite Table Name: `Largest_banks`
## Prerequisites
### Before running the pipeline, make sure you have the following Python libraries installed:

- `pandas`
- `numpy`
- `requests`
- `beautifulsoup4`
- `apache-airflow`
### You can install them using pip:

```bash
Copy code
pip install pandas numpy requests beautifulsoup4 apache-airflow
```
## ETL Pipeline Steps
### The ETL pipeline follows these steps:

1. Extract: Fetches data from the specified URL and converts it into a pandas DataFrame.
2. Transform: Reads exchange rate data from a CSV file and calculates the market cap values in different currencies (GBP, EUR, INR).
3. Load to CSV: Saves the transformed data to a CSV file.
4. Load to SQLite Database: Inserts the transformed data into an SQLite database.
5. Query Execution: Executes queries on the SQLite database to retrieve and analyze data.
## Airflow DAG
### This project uses Apache Airflow to manage the ETL process. The DAG (Directed Acyclic Graph) is set up with the following tasks:

- `log_task`: Logs the start of the ETL process.
- `extract_task`: Handles data extraction from the URL.
- `transform_task`: Transforms the data using exchange rates.
- `load_csv_task`: Saves the transformed data to a CSV file.
- `load_db_task`: Loads the data into an SQLite database.
### The tasks are executed in this order:

1. Log progress
2. Extract data
3. Transform data
4. Load data into CSV and SQLite
## How to Run the DAG
1. Ensure Apache Airflow is installed and running.
2. Place the Python script in your Airflow DAGs folder.
3. Trigger the DAG manually from the Airflow UI to start the ETL process.
## Expected Outputs
- CSV File: The transformed data will be saved in largest_banks.csv.
- SQLite Database: The database Banks.db will contain a table named Largest_banks with the processed data.
## Logging
All progress and error messages are recorded in the log file specified by log_path.

## Summary
- This ETL pipeline project automates the process of extracting, transforming, and loading bank data using Apache Airflow.
- By integrating data from a web source, applying currency exchange rates, and storing the results in both CSV and SQLite formats, this project demonstrates an efficient approach to managing and analyzing financial data.
- The use of Airflow ensures reliable execution and orchestration of the ETL tasks, making it a robust solution for data processing and analysis.
