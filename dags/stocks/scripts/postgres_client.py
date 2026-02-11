import psycopg2

PG_HOST = 'postgres-business'
PG_USER = 'data_eng'
PG_PASSWORD = 'data_eng_pwd'
PG_DB = 'staging_db'


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )


def init_pg_table():
    create_query = """
    CREATE TABLE IF NOT EXISTS stock_prices_pg (
        ticker VARCHAR(10),
        date TIMESTAMP, 
        open FLOAT,
        close FLOAT,
        high FLOAT,
        low FLOAT,
        volume FLOAT,
        inserted_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (ticker, date)
    );
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_query)
                conn.commit()
    except psycopg2.errors.UniqueViolation:
        print("Таблица уже создана параллельным процессом. Идем дальше.")
    except Exception as e:
        print(f"Критическая ошибка при создании таблицы: {e}")
        raise e


def insert_pg_data(ticker, candles):
    insert_query = """
        INSERT INTO stock_prices_pg (ticker, date, open, close, high, low, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    data_to_insert = []
    for row in candles:
        date_str = row[6]
        data_to_insert.append((
            ticker, date_str, row[0], row[1], row[2], row[3], row[5]
        ))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_query, data_to_insert)
            conn.commit()


def clear_pg_data(ticker, start_date, end_date):

    query = "DELETE FROM stock_prices_pg WHERE ticker = %s AND date >= %s AND date < %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticker, start_date, end_date))
            conn.commit()