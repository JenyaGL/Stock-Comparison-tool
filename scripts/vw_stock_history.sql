CREATE OR REPLACE VIEW `red-function-478012-q6.stock_data.vw_stock_history`()
BEGIN

SELECT
  name,
  country,
  date,
  current_price,
  percent_change,
  price_range,
  fetched_date
FROM stock_data.filtered_stock_data_layer
ORDER BY name, date

END;
