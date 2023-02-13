import logging
import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from PostgreSQLCountRows_Operator import PostgreSQLCountRowsOperator

logging.basicConfig(filename='std.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

default_args = {
    'owner': 'Peter',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 29),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'postgres_conn_id': 'postgres_local'
}

config = {
    'table_name_1': {'schedule_interval': None, "start_date": datetime(2022, 12, 29), "table_name": "table_name_1"},
    'table_name_2': {'schedule_interval': None, "start_date": datetime(2022, 12, 29), "table_name": "table_name_2"},
    'table_name_3': {'schedule_interval': None, "start_date": datetime(2022, 12, 29), "table_name": "table_name_3"}
}

for dag_id in config:
    with DAG(
            dag_id, start_date=config[dag_id]["start_date"], default_args=default_args,
            schedule=config[dag_id]["schedule_interval"], catchup=False
    ) as dag:
        def log():
            logging.info(f'{dag_id} start processing tables in database: {config[dag_id]["table_name"]}')


        def check_table_exist():
            sql = "SELECT EXISTS (SELECT FROM new_table);"
            hook = PostgresHook(postgres_conn_id='postgres_local')
            first_col_of_first_row = hook.get_first(sql)[0]
            if first_col_of_first_row:
                return 'insert_row'
            else:
                return 'create_table'


        print_process_start = PythonOperator(task_id='print_process_start', python_callable=log,
                                             queue='jobs_queue')

        get_current_user = BashOperator(task_id='get_current_user', bash_command='whoami',
                                        do_xcom_push=True, queue='jobs_queue')

        check_table_exists = BranchPythonOperator(
            task_id='check_table_exists', queue='jobs_queue',
            python_callable=check_table_exist
        )
        print_process_start >> get_current_user >> check_table_exists

        create_table = PostgresOperator(
            task_id='create_table', queue='jobs_queue',
            postgres_conn_id='postgres_local',
            sql='''CREATE TABLE new_table(
            custom_id integer NOT NULL, 
            user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL
            );''',
        )

        insert_row = PostgresOperator(
            task_id='insert_row', queue='jobs_queue',
            trigger_rule=TriggerRule.NONE_FAILED,
            postgres_conn_id='postgres_local',
            sql='''INSERT INTO new_table VALUES
           (%s, '{{ ti.xcom_pull(task_ids='get_current_user', key='return_value') }}', %s);''', # UPSERT
            parameters=(uuid.uuid4().int % 123456789, datetime.now()))

        check_table_exists >> [create_table, insert_row]

        insert_row << [create_table, check_table_exists]

        query_the_table = PostgreSQLCountRowsOperator(task_id='query_the_table', do_xcom_push=True, queue='jobs_queue')

        insert_row >> query_the_table
