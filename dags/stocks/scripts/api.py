import requests


def get_moex_data(ticker, start_date, end_date, interval):

    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json"

    params = {
        "from": start_date,
        "till": end_date,
        "interval": interval
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Ошибка API MOEX для {ticker}: {e}")
        raise e