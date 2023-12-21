from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests

# Определение параметров DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': timedelta(hours=3),
    'max_active_runs': 1,
    'catchup': False,
}

# Инициализация DAG
dag = DAG(
    'bitcoin_etl',
    default_args=default_args,
    description='ETL process for BTC/USD exchange rate',
    schedule_interval=timedelta(hours=3),
)


# Функция для извлечения данных
def extract_data(**kwargs):
    access_key = "bf162405090514714be68bbb866079d4"
    source = "BTC"
    currencies = "USD"
    start_date = "2023-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    url = f"http://api.exchangerate.host/timeframe?"
    url += f"access_key={access_key}&"
    url += f"start_date={start_date}&"
    url += f"end_date={end_date}&"
    url += f"source={source}&"
    url += f"currencies={currencies}&"
    url += f"format=1"

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

# Функция для создания таблицы
def create_table_if_not_exists():
    pg_hook = PostgresHook(postgres_conn_id='postgres_data_connection')
    check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'exchange_rate'
        )
    """
    check_constraint_sql = """
        SELECT EXISTS (
            SELECT CONSTRAINT_NAME 
            FROM information_schema.table_constraints 
            WHERE CONSTRAINT_NAME = 'exchange_rate_unique_constraint'
        )
    """

    table_exists = pg_hook.get_first(check_table_sql)[0]
    constraint_exists = pg_hook.get_first(check_constraint_sql)[0]

    if not table_exists:
        create_table_sql = """
            CREATE TABLE exchange_rate (
                id SERIAL PRIMARY KEY,
                currency_pair VARCHAR(255),
                date DATE,
                rate NUMERIC
            )
        """
        pg_hook.run(create_table_sql)

    if not constraint_exists:
        add_constraint_sql = """
            ALTER TABLE exchange_rate
            ADD CONSTRAINT exchange_rate_unique_constraint
            UNIQUE (currency_pair, date)
        """
        pg_hook.run(add_constraint_sql)


# Функция для загрузки данных в БД
def load_data_to_db(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')

    currency_pair = 'BTC/USD'
    quotes = data.get('quotes', {})

    pg_hook = PostgresHook(postgres_conn_id='postgres_data_connection')

    for date, quote_data in quotes.items():
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        exchange_rate = quote_data.get('BTCUSD')

        if exchange_rate is not None:
            # Используем INSERT ... ON CONFLICT для обновления существующих записей
            sql = f"INSERT INTO exchange_rate (currency_pair, date, rate) VALUES ('{currency_pair}', '{date_obj}', {exchange_rate}) ON CONFLICT (currency_pair, date) DO UPDATE SET rate = EXCLUDED.rate"
            pg_hook.run(sql)


# Задача извлечения данных
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Задача для создания таблицы
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    provide_context=True,
    dag=dag,
)

# Задача загрузки данных в БД
load_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data_to_db,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
extract_task >> create_table_task >> load_task

