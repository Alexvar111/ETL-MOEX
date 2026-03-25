INSERT INTO stock_prices_analytical (ticker, date, open, close, high, low, volume, inserted_at)
SELECT ticker, date, open, close, high, low, volume, inserted_at
FROM pg_raw_link
WHERE ticker = '{{ task.ticker }}' 
  AND date >= '{{ data_interval_start | ds }}' 
  AND date < '{{ data_interval_end | ds }}';