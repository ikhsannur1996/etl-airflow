# Airflow DAG Exercise

## 1. DAG from CSV to Database:

### Description:
This DAG automates the process of extracting data from one or more CSV files, performing any necessary data transformations, and loading the transformed data into a target database. The DAG consists of three main tasks: 
- **Extract Data**: Reads the CSV file(s) and loads the data into memory.
- **Transform Data**: Applies any required transformations to the extracted data.
- **Load Data to Database**: Inserts the transformed data into the target database table. 

### Task Details:
- **Extract Data**: This task reads data from CSV file(s) located at a specified path. The data is loaded into a pandas DataFrame.
- **Transform Data**: This task applies any necessary data transformations to the extracted data. Examples of transformations include data cleansing, normalization, or aggregation.
- **Load Data to Database**: This task inserts the transformed data into the target database table. It ensures that the target table is created if it does not already exist.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 1),
}

# Function to extract data from CSV
def extract_data():
    data = pd.read_csv('/path/to/csv/file.csv')
    return data

# Function to transform data
def transform_data(data):
    transformed_data = data  # Placeholder, replace with actual transformations
    return transformed_data

# Function to load data into database
def load_data_to_database(transformed_data):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_create_query = """
    CREATE TABLE IF NOT EXISTS target_table (
        column1 TYPE,
        column2 TYPE,
        ...
    );
    """
    postgres_hook.run(table_create_query)
    
    postgres_hook.insert_rows(table='target_table', rows=transformed_data.values.tolist())

# Define the DAG
with DAG('csv_to_database_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_data_to_database',
        python_callable=load_data_to_database,
        provide_context=True
    )
    
    extract_task >> transform_task >> load_task
```

## 2. DAG from API to Database:

### Description:
This DAG automates the process of fetching data from one or more APIs, transforming the retrieved data, and loading it into a target database. It consists of similar tasks to the CSV to Database DAG, with the main difference being the source of the data.

### Task Details:
- **Fetch Data from API**: This task makes HTTP requests to one or more APIs to retrieve data. The data is typically returned in JSON format.
- **Transform Data**: Similar to the CSV DAG, this task applies any necessary data transformations to the retrieved data.
- **Load Data to Database**: Inserts the transformed data into the target database table. Automatic table creation is ensured if necessary.

```python
# Similar structure as CSV to Database DAG, but replace CSV extraction with API calls.
# Transform data and load into database. Also, ensure automatic table creation in the target database.
# Define appropriate functions and operators for each task.
```

## 3. DAG to Call Stored Procedure:

### Description:
This DAG triggers the execution of a stored procedure in a database. It is useful for automating routine database operations or executing complex business logic stored in the database.

### Task Details:
- **Call Stored Procedure**: This task uses the PostgresOperator (or appropriate operator for the database type) to execute a predefined stored procedure in the target database. It ensures that the execution status of the stored procedure is monitored.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 1),
}

# Define the DAG
with DAG('call_stored_procedure_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    call_stored_procedure_task = PostgresOperator(
        task_id='call_stored_procedure',
        postgres_conn_id='postgres_default',
        sql="CALL your_stored_procedure();"
    )
```

## 4. DAG to Call Query:

### Description:
This DAG executes predefined SQL queries on a database. It is commonly used for performing data manipulation or retrieval operations on the database.

### Task Details:
- **Call Query**: This task executes predefined SQL queries using the PostgresOperator (or equivalent operator). It handles the execution of queries and ensures proper error handling and logging.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 1),
}

# Define the DAG
with DAG('call_query_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    call_query_task = PostgresOperator(
        task_id='call_query',
        postgres_conn_id='postgres_default',
        sql="SELECT * FROM your_table;"
    )
```

## 5. DAG from Database to Database (Extract >> Transform >> Load):

### Description:
This DAG automates the process of extracting data from a source database, applying necessary transformations, and loading the transformed data into a target database. It is useful for data migration or synchronization between databases.

### Task Details:
- **Extract Data from Source Database**: This task extracts data from a source database table(s) using the appropriate database hook. The extracted data is typically stored in memory.
- **Transform Data**: This task applies necessary transformations to the extracted data, such as data cleansing or aggregation.
- **Load Data to Target Database**: This task inserts the transformed data into the target database table. It ensures that the target table is created if it does not already exist.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers

.postgres.hooks.postgres import PostgresHook

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 1),
}

# Function to extract data from source database
def extract_data_from_source():
    postgres_hook = PostgresHook(postgres_conn_id='source_db_conn')
    return postgres_hook.get_records(sql="SELECT * FROM source_table;")

# Function to transform data
def transform_data(data):
    transformed_data = data  # Placeholder, replace with actual transformations
    return transformed_data

# Function to load data into target database
def load_data_to_target(transformed_data):
    postgres_hook = PostgresHook(postgres_conn_id='target_db_conn')
    
    table_create_query = """
    CREATE TABLE IF NOT EXISTS target_table (
        column1 TYPE,
        column2 TYPE,
        ...
    );
    """
    postgres_hook.run(table_create_query)
    
    postgres_hook.insert_rows(table='target_table', rows=transformed_data)

# Define the DAG
with DAG('db_to_db_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data_from_source',
        python_callable=extract_data_from_source
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_data_to_target',
        python_callable=load_data_to_target,
        provide_context=True
    )
    
    extract_task >> transform_task >> load_task
```

```

This combined document provides both the Airflow DAG scripts and detailed descriptions for each DAG, making it a comprehensive resource for understanding and implementing the data pipeline automation tasks.
