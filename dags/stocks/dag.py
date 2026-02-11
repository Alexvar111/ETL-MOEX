from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from stocks.config import tickers
from stocks.scripts.manager import load_to_postgres, transfer_to_clickhouse

for ticker_info in tickers:
    ticker_code = ticker_info["ticker"]
    ticker_interval = ticker_info["interval"]

    dag_id = f"moex_loader_{ticker_code}_elt"

    dag = DAG(
        dag_id=dag_id,
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        max_active_runs=1,
        catchup=False,
        tags=["moex", "postgres", "clickhouse", "elt"]
    )

    with dag:
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        task_to_pg = PythonOperator(
            task_id="load_to_postgres",
            python_callable=load_to_postgres,
            op_kwargs={
                "ticker": ticker_code,
                "interval": ticker_interval
            },
        )

        task_to_ch = PythonOperator(
            task_id="transfer_to_clickhouse",
            python_callable=transfer_to_clickhouse,
            op_kwargs={
                "ticker": ticker_code
            },
        )

        start >> task_to_pg >> task_to_ch >> end

    globals()[dag_id] = dag