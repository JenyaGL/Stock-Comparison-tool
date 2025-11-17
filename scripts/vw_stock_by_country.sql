CREATE OR REPLACE VIEW 'red-function-478012-q6.stock_data.vw_stock_by_country'()
BEGIN

SELECT
  country,
  date,
  COUNT(DISTINCT name) AS ticker_count,
  ROUND(AVG(current_price), 2) AS avg_current_price,
  ROUND(AVG(price_change), 2) AS avg_price_change,
  ROUND(AVG(percent_change), 2) AS avg_percent_change
FROM stock_data.filtered_stock_data_layer
GROUP BY country, date
ORDER BY date DESC, country

END;
