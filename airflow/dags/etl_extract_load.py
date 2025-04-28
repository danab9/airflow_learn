from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import re
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='et_extract_load',
    description='Extract and load data from public to staging',
    schedule_interval=None,
    start_date=days_ago(1),
)

# Helper functions
def parse_delta_column(value, delta_type):
    if value is None:
        return datetime(1900, 1, 1) if delta_type == 'date' else ''
    if delta_type == 'date':
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    return str(value)

def get_max_delta(rows, source_columns, delta_column):
    try:
        delta_index = source_columns.index(delta_column)
        return max(row[delta_index] for row in rows)
    except ValueError:
        raise Exception(f"Delta column '{delta_column}' not found in source_columns.")
    except IndexError:
        raise Exception("Row does not contain enough columns to access the delta value.")

# Functions
def get_last_load(table_name, delta_type, **kwargs):
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
            last_load = '1970-01-01 00:00:00' if delta_type == 'date' else '0'
        print(f"Last load id: {last_load}")
        kwargs['ti'].xcom_push(key='last_load', value=last_load)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()

def query_and_insert_staging(source_table, staging_table, delta_column, delta_type, columns, **kwargs):
    ti = kwargs['ti']
    last_load = ti.xcom_pull(task_ids='get_last_load', key='last_load')

    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()

    try:
        cursor = conn.cursor()
        col_str = ', '.join(columns)
        delta_value = parse_delta_column(last_load, delta_type)
        
        cursor.execute(
                f"""
                SELECT {col_str}
                FROM public.{source_table}
                WHERE {delta_column} > %s;
                """, (delta_value,)
        )  # Make sure last_load is passed as a string
        
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} new rows from {source_table}.") 

        placeholders = ', '.join(['%s'] * len(columns)) # create placeholders for the number of columns
        insert_sql = f"""
            INSERT INTO "Staging"."{staging_table}" ({col_str})
            VALUES ({placeholders});
        """

        for row in rows:
            cursor.execute(insert_sql, row)

        if rows:
            max_id = get_max_delta(rows, columns, delta_column)
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
        """, (table_name, str(max_id)))
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

def push_products_to_core():
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

    # Insert transformed data into the "Core" table
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO "core"."dim_product" (product_id, product_name, category, subcategory, brand)
        VALUES (%s, %s, %s, %s, %s)
        """
        pg_hook.run(insert_sql, parameters=(row['product_id'], row['product_name'], row['category'], row['subcategory'], row['brand']))

    print("Data transformation and insertion to Core complete.")
    
def update_dim_payment():
    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT
                COALESCE(payment, 'cash') AS payment,
                loyalty_card
            FROM "Staging".sales
        """)
        staging_rows = cursor.fetchall()

        # Get existing rows from dim_payment
        cursor.execute("""
            SELECT payment, loyalty_card
            FROM core.dim_payment
        """)
        existing_rows = set(cursor.fetchall())
        new_rows = [row for row in staging_rows if row not in existing_rows]

        for row in new_rows:
            cursor.execute("""
                INSERT INTO core.dim_payment (payment, loyalty_card)
                VALUES (%s, %s)
            """, row)

        conn.commit()
        print(f"Inserted {len(new_rows)} new rows into dim_payment.")
    except Exception as e:  
        print(f"Error updating dim_payment: {e}")
    finally:
        cursor.close()

def transform_fact_sales_to_core(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='PostgresSQL_connection_1')
    conn = pg_hook.get_conn()

    sql = """
            INSERT INTO core.sales (
                transaction_id,
                transactional_date,
                transactional_date_fk,
                product_id,
                product_fk,
                payment_fk,
                customer_id,
                credit_card,
                cost,
                quantity,
                price,
                total_price,
                total_cost,
                profit
            )
            SELECT DISTINCT ON (f.transaction_id)
                f.transaction_id,
                f.transactional_date,
                EXTRACT(YEAR FROM f.transactional_date) * 10000 + EXTRACT(MONTH FROM f.transactional_date) * 100 + EXTRACT(DAY FROM f.transactional_date),
                f.product_id,
                p.product_PK,
                d.payment_PK,
                f.customer_id,
                f.credit_card,
                f.cost,
                f.quantity,
                f.price,
                (f.price * f.quantity),
                (f.cost * f.quantity),
                (f.price * f.quantity - f.cost * f.quantity)
            FROM "Staging".sales f
            LEFT JOIN core.dim_payment d ON d.payment = COALESCE(f.payment, 'cash') AND d.loyalty_card = f.loyalty_card
            LEFT JOIN core.dim_product p ON p.product_id = f.product_id
            ORDER BY f.transaction_id, f.transactional_date DESC;
"""

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print("Successfully inserted data into core.sales")

    except Exception as e:  
        print(f"Error fetching data from sales: {e}")
    finally:
        cursor.close()


product_params = {
    'source_table': 'products',
    'staging_table': 'dim_product',
    'delta_column': 'product_id',
    'delta_type' : 'string',
    'source_columns': ['product_id', 'product_name', 'category', 'subcategory'],
    'core_columns': ['product_id', 'product_name', 'category', 'subcategory']
}

sales_params = {
    'source_table': 'sales',
    'staging_table': 'sales',
    'delta_column': 'transactional_date',
    'delta_type' : 'date',
    'source_columns': ['transaction_id', 'transactional_date', 'product_id', 'customer_id', 'payment', 'credit_card', 
                        'loyalty_card', 'cost', 'quantity', 'price'],
    'core_columns': ['transaction_id', 'transactional_date', 'transactional_date_fk', 'product_id', 
                        'product_fk','customer_id', 'payment_fk', 'credit_card', 
                        'cost','quantity','price', 'total_cost', 'total_price', 'profit']
}

# 1. products table
truncate = PythonOperator(
    task_id=f'truncate_{product_params["staging_table"]}',
    python_callable=truncate_staging,
    op_kwargs={'table_name': product_params['staging_table']},
    dag=dag,
)

get_last = PythonOperator(
    task_id=f'get_last_load_{product_params["staging_table"]}',
    python_callable=get_last_load,
    op_kwargs={'table_name': product_params['staging_table'], 'delta_type': product_params['delta_type']},
    dag=dag,
)

insert = PythonOperator(
    task_id=f'query_and_insert_{product_params["staging_table"]}',
    python_callable=query_and_insert_staging,
    op_kwargs={
        'source_table': product_params['source_table'],
        'staging_table': product_params['staging_table'],
        'delta_column': product_params['delta_column'],
        'delta_type': product_params['delta_type'],
        'columns': product_params['source_columns'],
    },
    dag=dag,
)

metadata = PythonOperator(
    task_id=f'update_metadata_{product_params["staging_table"]}',
    python_callable=update_metadata,
    op_kwargs={'table_name': product_params['staging_table'], 'task_id': f'query_and_insert_{product_params["staging_table"]}'},
    dag=dag,
)

transform_products_insert_core = PythonOperator(
    task_id='transform_products_insert_core',
    python_callable=push_products_to_core,
    dag=dag,
)

truncate >> get_last >> insert >> metadata >> transform_products_insert_core 

# 2. sales table
truncate = PythonOperator(
    task_id=f'truncate_{sales_params["staging_table"]}',
    python_callable=truncate_staging,
    op_kwargs={'table_name': sales_params['staging_table']},
    dag=dag,
)

get_last = PythonOperator(
    task_id=f'get_last_load_{sales_params["staging_table"]}',
    python_callable=get_last_load,
    op_kwargs={'table_name': sales_params['staging_table'], 'delta_type': sales_params['delta_type']},
    dag=dag,
)

insert = PythonOperator(
    task_id=f'query_and_insert_{sales_params["staging_table"]}',
    python_callable=query_and_insert_staging,
    op_kwargs={
        'source_table': sales_params['source_table'],
        'staging_table': sales_params['staging_table'],
        'delta_column': sales_params['delta_column'],
        'delta_type': sales_params['delta_type'],
        'columns': sales_params['source_columns'],
    },
    dag=dag,
)

metadata = PythonOperator(
    task_id=f'update_metadata_{sales_params["staging_table"]}',
    python_callable=update_metadata,
    op_kwargs={'table_name': sales_params['staging_table'], 'task_id': f'query_and_insert_{sales_params["staging_table"]}'},
    dag=dag,
)

dim_payment = PythonOperator(
    task_id='update_dim_payment',
    python_callable=update_dim_payment,
    dag=dag,
)

transform_sales_insert_core = PythonOperator(
    task_id='transform_facts_insert_core',
    python_callable=transform_fact_sales_to_core,
    dag=dag,
)


truncate >> get_last >> insert >> metadata >> dim_payment >> transform_sales_insert_core
