CREATE OR REPLACE PROCEDURE `red-function-478012-q6.stock_data.daily_stock_pipeline`()
BEGIN
  CALL stock_data.load_new_data_from_external_json();
  CALL stock_data.raw_stock_data_layer();
  CALL stock_data.filtered_stock_data_layer();
END;
