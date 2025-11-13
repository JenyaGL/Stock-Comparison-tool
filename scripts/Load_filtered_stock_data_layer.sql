CREATE OR REPLACE PROCEDURE `red-function-478012-q6.stock_data.filtered_stock_data_layer`()
BEGIN

-- DDL of filtered_stock_data_layer table
INSERT into stock_data.filtered_stock_data_layer(name, country, current_price, price_change, percent_change, high, low, open, prev_close, price_range, date, time, fetched_date)

-- 
 WITH ranked AS (
      SELECT *,
          ROW_NUMBER() OVER (
              PARTITION BY symbol, CAST(fetched_at AS DATETIME)
              ORDER BY fetched_at DESC
          ) AS rn
      FROM stock_data.raw_stock_data_layer
  )

  SELECT
    UPPER(TRIM(symbol)) AS name,
    UPPER(TRIM(country)) AS country,
    COALESCE(c, 0) AS current_price,
    COALESCE(d, 0) AS price_change,
    COALESCE(dp, 0) AS percent_change,
    COALESCE(h, 0) AS high,
    COALESCE(l, 0) AS low,
    COALESCE(o, 0) AS open,
    COALESCE(pc, 0) AS prev_close,
    ROUND(CAST(h AS FLOAT64) - CAST(l AS FLOAT64), 3) AS price_range,
    CAST(fetched_at AS DATE) AS date,
    TIME(fetched_at) AS time,
    CAST(fetched_at AS DATETIME) AS fetched_date
    
  FROM ranked
  WHERE rn = 1;

END;
