import requests
import time
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class MoexToPgOperator(BaseOperator):

    template_fields = ('ticker', 'api_start_date', 'api_end_date')

    def __init__(
            self,
            ticker: str,
            interval: int,
            api_start_date: str,
            api_end_date: str,
            postgres_conn_id: str = 'postgres_business',
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ticker = ticker
        self.interval = interval
        self.api_start_date = api_start_date
        self.api_end_date = api_end_date
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        self.log.info(f"Запрашиваем API MOEX для {self.ticker} с {self.api_start_date} по {self.api_end_date}")

        url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{self.ticker}/candles.json"

        params = {"from": self.api_start_date, "till": self.api_end_date, "interval": self.interval}
        headers = {"User-Agent": "Mozilla/5.0"}

        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        time.sleep(0.5)

        candles = response.json().get('candles', {}).get('data', [])

        if not candles:
            self.log.info("Данных за этот период нет, пропускаем загрузку.")
            return

        self.log.info(f"Получено {len(candles)} строк. Загружаем в Postgres...")

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        insert_query = """
            INSERT INTO stock_prices_pg (ticker, date, open, close, high, low, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open,
                close = EXCLUDED.close,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                volume = EXCLUDED.volume
        """

        data_to_insert = [
            (self.ticker, row[6], row[0], row[1], row[2], row[3], row[5])
            for row in candles
        ]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_query, data_to_insert)
                conn.commit()

        self.log.info("Успешно загружено в Staging.")