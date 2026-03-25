DELETE FROM stock_prices_pg 
WHERE ticker = '{{ params.ticker }}' 
  AND date >= '{{ data_interval_start | ds }}' 
  AND date < '{{ data_interval_end | ds }}';