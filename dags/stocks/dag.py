from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from stocks.config import tickers
from stocks.scripts.manager import fetch_and_load


for ticker_info in tickers:
    ticker_code = ticker_info["ticker"]
    ticker_interval = ticker_info["interval"]

    dag_id = f"moex_loader_{ticker_code}"

    dag = DAG(
        dag_id=dag_id,
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        max_active_runs=1,
        catchup=False,
        tags=["moex", "dynamic"]
    )


    with dag:
        start = EmptyOperator(task_id="start")

        task_load = PythonOperator(
            task_id="fetch_and_load_clickhouse",
            python_callable=fetch_and_load,
            op_kwargs={
                "ticker": ticker_code,
                "interval": ticker_interval
            },
        )

        end = EmptyOperator(task_id="end")

        start >> task_load >> end


    globals()[dag_id] = dag