CREATE OR REPLACE PROCEDURE `red-function-478012-q6.stock_data.load_new_data_from_external_json`()
BEGIN

  -- Insert only new records based on symbol + fetched_at
  INSERT INTO stock_data.raw_stock_data_layer (c, d, dp, h, l, o, pc, t, symbol, fetched_at, country)
  
  SELECT 
  c, d, dp, h, l, o, pc, t, symbol, fetched_at, country
  FROM stock_data.temp_external_json ext
  WHERE NOT EXISTS (
                    SELECT 1
                    FROM stock_data.raw_stock_data_layer existing
                    WHERE existing.symbol = ext.symbol
                    AND existing.fetched_at = ext.fetched_at);
                    END;
