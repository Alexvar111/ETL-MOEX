from airflow.models.baseoperator import BaseOperator

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

class PostgresToClickhouseOperator(BaseOperator):
    template_fields = ('sql_script', 'ticker')
    template_ext = ('.sql',)

    def __init__(
        self,
        ticker: str,
        sql_script: str,
        clickhouse_conn_id: str = 'clickhouse_business',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ticker = ticker
        self.sql_script = sql_script
        self.clickhouse_conn_id = clickhouse_conn_id

    def execute(self, context):
        self.log.info(f"Запуск переноса данных в ClickHouse для тикера: {self.ticker}")
        self.log.info(f"Выполняем SQL: \n{self.sql_script}")

        # Hook берет пароли из Airflow Connections
        ch_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        ch_hook.execute(self.sql_script)

        self.log.info("Перенос успешно завершен.")