"""
DAG: сбор данных с Московской биржи → JSON
Конфиг читается из /opt/airflow/config/stocks.json
Результат сохраняется в /opt/airflow/output/
"""
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moex_stock_prices',
    default_args=default_args,
    description='Забираем котировки акций с MOEX',
    schedule_interval='30 16 * * *',  # Ежедневно в 19:30 по МСК (UTC+3)
    start_date=datetime(2025, 2, 5),
    catchup=False,
    tags=['moex', 'stocks'],
)

CONFIG_PATH = '/opt/airflow/config/stocks.json'
OUTPUT_DIR = '/opt/airflow/output'


def fetch_stock_data(**context):
    execution_date = context['ds']  # Логическая дата DAG, например: '2025-02-06'

    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = json.load(f)

    tickers = config['tickers']
    data = {}

    for ticker in tickers:
        url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"
        params = {
            "from": execution_date,
            "till": execution_date,
            "interval": 24
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                json_data = response.json()
                candles = json_data.get('candles', {}).get('data', [])
                if candles:
                    last = candles[-1]
                    data[ticker] = {
                        "timestamp": last[0],
                        "open": last[1],
                        "close": last[2],
                        "low": last[3],
                        "high": last[4],
                        "volume": last[6]
                    }
                else:
                    data[ticker] = "Нет данных за указанный период"
            else:
                data[ticker] = f"HTTP {response.status_code}"
        except Exception as e:
            data[ticker] = f"Ошибка: {str(e)}"

    output_file = f"{OUTPUT_DIR}/stock_data_{execution_date.replace('-', '')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Данные сохранены: {output_file}")


fetch_task = PythonOperator(
    task_id='fetch_moex_data',
    python_callable=fetch_stock_data,
    provide_context=True,
    dag=dag,
)