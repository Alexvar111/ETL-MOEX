from airflow.models.baseoperator import BaseOperator

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

class PostgresToClickhouseOperator(BaseOperator):
    """
    Кастомный оператор для выполнения SQL-скриптов внутри ClickHouse.
    Используется для переливки данных из PostgreSQL (через Engine=PostgreSQL)
    в аналитические витрины (ReplacingMergeTree).

    :param ticker: Тикер акции (для шаблонизации SQL запроса).
    :param sql_script: Текст SQL-запроса или путь к .sql файлу.
    :param clickhouse_conn_id: ID подключения к ClickHouse в Airflow Connections.
    """

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
        """
        Инициализирует ClickHouseHook и выполняет переданный SQL-скрипт.
        """

        self.log.info(f"Запуск переноса данных в ClickHouse для тикера: {self.ticker}")
        self.log.info(f"Выполняем SQL: \n{self.sql_script}")

        # Hook берет пароли из Airflow Connections
        ch_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        ch_hook.execute(self.sql_script)

        self.log.info("Перенос успешно завершен.")