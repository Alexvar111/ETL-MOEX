from stocks.scripts.api import get_moex_data
from stocks.scripts.postgres_client import init_pg_table, clear_pg_data, insert_pg_data
from stocks.scripts.clickhouse_client import init_ch_tables, clear_ch_data, transfer_data_from_pg


def load_to_postgres(ticker, interval, **context):

    start_date = context["data_interval_start"].strftime('%Y-%m-%d')
    end_date = context["data_interval_end"].strftime('%Y-%m-%d')

    print(f"--- Запуск загрузки в Postgres: {ticker} ({start_date}) ---")

    init_pg_table()

    json_response = get_moex_data(ticker, start_date, end_date, interval)
    candles = json_response.get('candles', {}).get('data', [])

    if not candles:
        print("Данных нет, пропускаем.")
        return

    clear_pg_data(ticker, start_date, end_date)

    insert_pg_data(ticker, candles)

    print(f"Успешно загружено строк: {len(candles)}")


def transfer_to_clickhouse(ticker, **context):
    start_date = context["data_interval_start"].strftime('%Y-%m-%d')
    end_date = context["data_interval_end"].strftime('%Y-%m-%d')

    print(f"--- Запуск переноса в ClickHouse: {ticker} ({start_date}) ---")

    init_ch_tables()

    clear_ch_data(ticker, start_date, end_date)

    transfer_data_from_pg(ticker, start_date, end_date)

    print("Перенос успешно завершен.")