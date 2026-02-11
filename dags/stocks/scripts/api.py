import requests
import time


def get_moex_data(ticker, start_date, end_date, interval):
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"

    params = {
        "from": start_date,
        "till": end_date,
        "interval": interval
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        time.sleep(0.5)

        return response.json()

    except Exception as e:
        print(f"Ошибка API MOEX для {ticker}: {e}")
        raise e