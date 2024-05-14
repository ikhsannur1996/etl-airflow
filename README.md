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
import numpy as np

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
    data = pd.read_csv('/opt/airflow/dags/dataset/employee_transaction.csv')
    return data

# Function to transform data
# Function to transform data
def transform_data(**kwargs):
    # Retrieve data from XCom
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    
    # Transform data  # Filter data where gender is 'male'
    transformed_data =  data[data['gender'] == 'Male']    
    
    # Pass transformed data to the next task
    return transformed_data


# Function to load data into database
def load_data_to_database(**kwargs):
    from airflow.hooks.postgres_hook import PostgresHook
    
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    
    table_exists_query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'ikhsan'
        AND table_name = 'male_employee'
    );
    """
    table_exists = postgres_hook.get_first(table_exists_query)[0]
    
    if not table_exists:
        # If table doesn't exist, create it
        create_query = f"""
        CREATE TABLE ikhsan.male_employee (
            {', '.join([f'{col} TEXT' for col in transformed_data.columns])}
        );
        """
        postgres_hook.run(create_query)
    
    # Insert data into the table
    insert_query = """
    INSERT INTO ikhsan.male_employee ({columns})
    VALUES %s
    """
    
    columns = ', '.join(transformed_data.columns)
    values = transformed_data.values.tolist()
    
    # Using execute_values to insert multiple rows
    from psycopg2.extras import execute_values
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    execute_values(cursor, insert_query.format(columns=columns), values)
    
    connection.commit()
    cursor.close()
    connection.close()


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
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

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

# Function to fetch data from API
def fetch_data_from_api():
    response = requests.get('https://api.sampleapis.com/simpsons/characters')
    data = response.json()
    return data

# Function to transform data
def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_api')
    # Transform JSON data to DataFrame
    df = pd.DataFrame(data)
    
    # Filter and transform data using pandas
    transformed_df = df[df['gender'] == 'f'] 
    
    return transformed_df

custom_schema = 'ikhsan'

# Function to load data into database
# Define the custom schema name
def load_data_to_database(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    # Retrieve connection from Airflow
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    
    # Analyze data types of transformed data
    data_types = {col: 'TEXT' for col, dtype in transformed_data.dtypes.items()}
    
    # Create table if it doesn't exist
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {custom_schema}.api_table (
        {', '.join([f'{col} {data_types[col]}' for col in transformed_data.columns])}
    );
    """
    postgres_hook.run(create_query)
    
    # Load data into the table with custom schema
    transformed_data.to_sql('api_table', postgres_hook.get_sqlalchemy_engine(), schema=custom_schema, if_exists='append', index=False)


# Define the DAG
with DAG('api_to_database_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    extract_task = PythonOperator(
        task_id='fetch_data_from_api',
        python_callable=fetch_data_from_api
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

## 3. DAG from Database to Database (Extract >> Transform >> Load):

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

def get_column_names(schema_name, table_name):
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    sql = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
    """
    columns = postgres_hook.get_records(sql)
    column_names = [col[0] for col in columns]
    return column_names

def extract_data_from_source():
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    data = postgres_hook.get_records(sql="SELECT * FROM public.employee;")
    return data

def transform_data(**context):
    data = context['ti'].xcom_pull(task_ids='extract_data_from_source')
    schema_name = 'public'
    table_name = 'employee'
    column_names = get_column_names(schema_name, table_name)
    
    df = pd.DataFrame(data, columns=column_names)
    
    # Apply your transformations here, e.g., filter, add columns, etc.
    transformed_df = df  # Placeholder for actual transformations
    
    # Convert DataFrame to list of dictionaries for loading
    transformed_data = transformed_df.to_dict(orient='records')
    return transformed_data

# Function to load data into target database
def load_data_to_target(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    if not transformed_data:
        raise ValueError("No data available to load.")
    
    # Retrieve target database connection
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    
    # Infer target table columns from transformed data
    target_columns = transformed_data[0].keys()
    
    # Automatically create table if it doesn't exist
    create_query = f"""
    CREATE TABLE IF NOT EXISTS ikhsan.employee (
        {', '.join([f'{col} TEXT' for col in target_columns])}
    );
    """
    postgres_hook.run(create_query)
    
    # Prepare rows for insertion
    rows = [tuple(record[col] for col in target_columns) for record in transformed_data]
    
    # Load data into target table
    postgres_hook.insert_rows(table='employee', rows=rows, scheme='ikhsan', target_fields=target_columns)

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
    
    # Create table using SQL query
    create_query = """
    CREATE TABLE IF NOT EXISTS "output".employee_output AS 
    SELECT *
    FROM public.employee;
    """
    create_task = PostgresOperator(
        task_id='extract_data',
        postgres_conn_id='postgres',
        sql=create_query
    )
    
    # Truncate table using SQL query
    truncate_query = """
    TRUNCATE TABLE "output".employee_output;
    """
    truncate_task = PostgresOperator(
        task_id='truncate_data',
        postgres_conn_id='postgres',
        sql=truncate_query
    )
    
    # Load data using SQL query
    insert_query = """
    INSERT INTO "output".employee_output 
    SELECT *
    FROM public.employee;
    """
    insert_task = PostgresOperator(
        task_id='insert_data_to_database',
        postgres_conn_id='postgres',
        sql=insert_query
    )
    
    # Define task dependencies
    create_task >> truncate_task >> insert_task
```

## 5. DAG to Call Stored Procedure:

### Description:
This DAG triggers the execution of a stored procedure in a database. It is useful for automating routine database operations or executing complex business logic stored in the database.

### Task Details:
- **Call Stored Procedure**: This task uses the PostgresOperator (or appropriate operator for the database type) to execute a predefined stored procedure in the target database. It ensures that the execution status of the stored procedure is monitored.

### Store Procedure:
```sql
-- DROP PROCEDURE public.create_employee();

CREATE OR REPLACE PROCEDURE public.create_employee()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    CREATE TABLE IF NOT EXISTS "output".employee_output AS 
    SELECT *
    FROM public.employee;
END;
$procedure$
;

-- DROP PROCEDURE public.insert_employee();

CREATE OR REPLACE PROCEDURE public.insert_employee()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO "output".employee_output 
    SELECT *
    FROM public.employee;
END;
$procedure$
;

-- DROP PROCEDURE public.truncate_employee();

CREATE OR REPLACE PROCEDURE public.truncate_employee()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    TRUNCATE TABLE "output".employee_output;
END;
$procedure$
;
```

### Calling Store Procedure DAG:
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
    
    call_stored_procedure_task_create = PostgresOperator(
        task_id='call_stored_procedure_crate',
        postgres_conn_id='postgres',
        sql="CALL public.create_employee();"
    )

    call_stored_procedure_task_truncate = PostgresOperator(
        task_id='call_stored_procedure_truncate',
        postgres_conn_id='postgres',
        sql="CALL public.truncate_employee();"
    )

    call_stored_procedure_task_insert = PostgresOperator(
        task_id='call_stored_procedure_insert',
        postgres_conn_id='postgres',
        sql="CALL public.insert_employee();"
    )

    call_stored_procedure_task_create >> call_stored_procedure_task_truncate >> call_stored_procedure_task_insert
```

These Airflow DAG scripts provide a foundation for automating various data pipeline tasks and database interactions. By leveraging Airflow's flexibility and scalability, users can customize and extend these workflows to suit their specific requirements and environments.
