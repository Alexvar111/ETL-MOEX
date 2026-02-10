from datetime import datetime
from clickhouse_driver import Client
from stocks.scripts.api import get_moex_data
from stocks.config import CLICKHOUSE_HOST


CH_USER = 'airflow'
CH_PASSWORD = 'airflow'


def init_clickhouse():

    client = Client(host=CLICKHOUSE_HOST, user=CH_USER, password=CH_PASSWORD)

    query = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        ticker String,
        date Date,
        open Float32,
        close Float32,
        high Float32,
        low Float32,
        volume Float32,
        inserted_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (ticker, date)
    """
    client.execute(query)


def fetch_and_load(ticker, interval, **context):

    start_date = context["data_interval_start"].strftime('%Y-%m-%d')
    end_date = context["data_interval_end"].strftime('%Y-%m-%d')

    print(f"Запуск для {ticker}. Период: {start_date} - {end_date}")

    init_clickhouse()

    json_response = get_moex_data(ticker, start_date, end_date, interval)

    candles_data = json_response.get('candles', {}).get('data', [])

    if not candles_data:
        print(f"Нет данных для {ticker} за этот период")
        return

    rows_to_insert = []
    for row in candles_data:
        date_str = row[6].split(' ')[0]

        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()

        rows_to_insert.append({
            'ticker': ticker,
            'date': date_obj,
            'open': float(row[0]),
            'close': float(row[1]),
            'high': float(row[2]),
            'low': float(row[3]),
            'volume': float(row[5])
        })

    client = Client(host=CLICKHOUSE_HOST, user=CH_USER, password=CH_PASSWORD)
    client.execute(
        'INSERT INTO stock_prices (ticker, date, open, close, high, low, volume) VALUES',
        rows_to_insert
    )
    print(f"Успешно загружено {len(rows_to_insert)} строк.")