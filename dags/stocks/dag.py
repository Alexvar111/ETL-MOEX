from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from stocks.config import tickers
from stocks.operators.moex_to_pg_operator import MoexToPgOperator
from stocks.operators.pg_to_ch_operator import PostgresToClickhouseOperator

for ticker_info in tickers:
    ticker_code = ticker_info["ticker"]
    ticker_interval = ticker_info["interval"]

    dag_id = f"moex_loader_{ticker_code}_elt"

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2025, 1, 1),
        schedule_interval="@daily",
        max_active_runs=1,
        catchup=False,
        tags=["moex", "postgres", "clickhouse", "elt"]
    ) as dag:

        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        # 1. Очистка старых данных (Идемпотентность)
        clear_pg = PostgresOperator(
            task_id="clear_staging_pg",
            postgres_conn_id="postgres_business",
            sql="sql/clear_staging.sql",
            params={"ticker": ticker_code}
        )

        # 2. Вызов API и загрузка в Postgres
        load_api_to_pg = MoexToPgOperator(
            task_id="load_api_to_pg",
            ticker=ticker_code,
            interval=ticker_interval,
            api_start_date="{{ data_interval_start | ds }}",
            api_end_date="{{ data_interval_end | ds }}",
            postgres_conn_id="postgres_business"
        )

        # 3. Перенос из Postgres в ClickHouse
        transfer_to_ch = PostgresToClickhouseOperator(
            task_id="transfer_pg_to_ch",
            ticker=ticker_code,
            sql_script="sql/transfer_to_ch.sql",
            clickhouse_conn_id="clickhouse_business"
        )

        start >> clear_pg >> load_api_to_pg >> transfer_to_ch >> end

    globals()[dag_id] = dag