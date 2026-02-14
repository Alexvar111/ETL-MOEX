from clickhouse_driver import Client
from stocks.config import CLICKHOUSE_HOST

CH_USER = 'airflow'
CH_PASSWORD = 'airflow'

PG_HOST = 'postgres-business'
PG_USER = 'data_eng'
PG_PASSWORD = 'data_eng_pwd'
PG_DB = 'staging_db'

def get_client():
    return Client(host=CLICKHOUSE_HOST, user=CH_USER, password=CH_PASSWORD)


def init_ch_tables():
    client = get_client()
    # ReplacingMergeTree: при merge оставляется одна строка на (ticker, date) — с макс. inserted_at.
    # Нет мутаций (ALTER DELETE), только INSERT; повторный запуск за тот же интервал даёт дубликаты, которые схлопнутся при merge.
    client.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices_analytical (
            ticker String,
            date DateTime,
            open Float32,
            close Float32,
            high Float32,
            low Float32,
            volume Float32,
            inserted_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(inserted_at)
        ORDER BY (ticker, date)
    """)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS pg_raw_link (
            ticker String,
            date DateTime,
            open Float32,
            close Float32,
            high Float32,
            low Float32,
            volume Float32,
            inserted_at DateTime
        ) ENGINE = PostgreSQL('{PG_HOST}:5432', '{PG_DB}', 'stock_prices_pg', '{PG_USER}', '{PG_PASSWORD}');
    """)

def transfer_data_from_pg(ticker, start_date, end_date):
    client = get_client()
    client.execute(f"""
        INSERT INTO stock_prices_analytical (ticker, date, open, close, high, low, volume, inserted_at)
        SELECT 
            ticker, date, open, close, high, low, volume, inserted_at
        FROM pg_raw_link
        WHERE ticker = '{ticker}' AND date >= '{start_date}' AND date < '{end_date}'
    """)