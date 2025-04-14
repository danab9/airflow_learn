from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import re

# Define the DAG
dag = DAG(
    dag_id='et_extract_load',
    description='Extract and load data from public to staging',
    schedule_interval=None,
    start_date=days_ago(1),
)

def get_last_load(table_name, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()

    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT last_load_id
            FROM "Staging"."etl_metadata"
            WHERE table_name = '{table_name}';
        """)
        result = cursor.fetchone()
        if result:
            last_load = result[0]
        else:
            last_load = 0
        print(f"Last load product_id: {last_load}")
        kwargs['ti'].xcom_push(key='last_load', value=last_load)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()

def query_and_insert_staging(source_table, staging_table, delta_column, source_columns, target_columns, **kwargs):
    ti = kwargs['ti']
    last_load = ti.xcom_pull(task_ids='get_last_load', key='last_load')

    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()

    try:
        cursor = conn.cursor()
        col_str = ', '.join(source_columns)
        cursor.execute(
                f"""
                SELECT {col_str}
                FROM public.{source_table}
                WHERE {delta_column} > %s;
                """, (str(last_load),)
        )  # Make sure last_load is passed as a string
        
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} new rows from {source_table}.") 

        placeholders = ', '.join(['%s'] * len(target_columns)) # create placeholders for the number of columns
        insert_sql = f"""
            INSERT INTO "Staging"."{staging_table}" ({col_str})
            VALUES ({placeholders});
        """

        for row in rows:
            cursor.execute(insert_sql, row)

        if rows:
            max_id = max(row[0] for row in rows)
            ti.xcom_push(key=f'max_id_{staging_table}', value=max_id)

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()

def update_metadata(table_name, task_id, **kwargs):
    ti = kwargs['ti']
    max_id = ti.xcom_pull(task_ids=task_id, key=f'max_id_{table_name}')

    if max_id is None:
        print(f"No new data for {table_name}, skipping metadatat update.")
        return
    
    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()

    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO "Staging"."etl_metadata" (table_name, last_load_id)
            VALUES (%s, %s)
            ON CONFLICT (table_name) DO UPDATE
            SET last_load_id = EXCLUDED.last_load_id;
        """, (table_name, max_id))
        conn.commit()
        print(f"Metadata updated: {table_name} -> {max_id}")
    except Exception as e:
        print(f"Error updating metadata for {table_name}: {e}")
    finally:
        cursor.close()
    
def truncate_staging(table_name, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f'TRUNCATE TABLE "Staging".{table_name};')
        conn.commit()
        print(f"{table_name} truncated.")
    except Exception as e:
        print(f"Error truncating {table_name}: {e}")
    finally:
        cursor.close()

def insert_transformed_to_core():
    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')

    sql = "SELECT * FROM \"Staging\".\"dim_product\";"
    df = pg_hook.get_pandas_df(sql)

    # Print the DataFrame before transformations
    print(f"Fetched {len(df)} rows from Staging. First few rows:")
    print(df.head())  # <-- Add this line to check the first few rows of the DataFrame

    # Transformations here
    df['product_name'] = df['product_name'].str.strip()  # remove leading and trailing spaces
    df['product_name'] = df['product_name'].str.replace('\t', '')  # remove tabs
    df['brand'] = df['product_name'].str.extract(r'\((.*?)\)', expand=False)  # extract brand from product_name
    df['product_name'] = df['product_name'].str.replace(r'\s*\(.*?\)', '', regex=True)  # remove brand from product_name

    # Print the transformed DataFrame before insertion
    print("Transformed data (first few rows):")
    print(df.head())  # <-- Add this line to check the transformed data

    # Insert transformed data into the "Core" table
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO "core"."dim_product" (product_id, product_name, category, subcategory, brand)
        VALUES (%s, %s, %s, %s, %s)
        """
        pg_hook.run(insert_sql, parameters=(row['product_id'], row['product_name'], row['category'], row['subcategory'], row['brand']))

    print("Data transformation and insertion to Core complete.")
    

# Define task parameters for each table
tables = [
    {
        'source_table': 'products',
        'staging_table': 'dim_product',
        'delta_column': 'product_id',
        'delta_type' : 'string',
        'source_columns': ['product_id', 'product_name', 'category', 'subcategory'],
        'target_columns': ['product_id', 'product_name', 'category', 'subcategory']
    },
    {
        'source_table': 'sales',
        'staging_table': 'sales',
        'delta_column': 'transactional_date',
        'delta_type' : 'date',
        'source_columns': ['transaction_id', 'transactional_date', 'product_id', 'customer_id', 'payment', 'credit_card', 
                          'loyalty_card', 'cost', 'quantity', 'price'],
        'target_columns': ['transaction_id', 'transactional_date', 'transactional_date_fk', 'product_id', 
                          'product_fk','customer_id', 'payment_fk', 'credit_card', 
                          'cost','quantity','price', 'total_cost', 'total_price', 'profit']
    }
]

task_objects = []

for t in tables:
    truncate = PythonOperator(
        task_id=f'truncate_{t["staging_table"]}',
        python_callable=truncate_staging,
        op_kwargs={'table_name': t['staging_table']},
        dag=dag,
    )

    get_last = PythonOperator(
        task_id=f'get_last_load_{t["staging_table"]}',
        python_callable=get_last_load,
        op_kwargs={'table_name': t['staging_table']},
        dag=dag,
    )

    insert = PythonOperator(
        task_id=f'query_and_insert_{t["staging_table"]}',
        python_callable=query_and_insert_staging,
        op_kwargs={
            'source_table': t['source_table'],
            'staging_table': t['staging_table'],
            'delta_column': t['delta_column'],
            'source_columns': t['source_columns'],
            'target_columns': t['target_columns'],
        },
        dag=dag,
    )

    update = PythonOperator(
        task_id=f'update_metadata_{t["staging_table"]}',
        python_callable=update_metadata,
        op_kwargs={'table_name': t['staging_table'], 'task_id': f'query_and_insert_{t["staging_table"]}'},
        dag=dag,
    )

    truncate >> get_last >> insert >> update

    # Optionally collect task objects for further use
    task_objects.extend([truncate, get_last, insert, update])