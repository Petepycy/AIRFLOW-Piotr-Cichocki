from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRowsOperator(BaseOperator):
    def __init__(self, postgres_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = "SELECT COUNT(*) FROM new_table;"
        result = hook.get_first(sql)
        message = f"There are {result} rows"
        print(message)
        return message
