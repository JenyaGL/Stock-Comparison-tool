CREATE OR REPLACE PROCEDURE stock_data.raw_stock_data_layer()

BEGIN

-- Loading of filtered_stock_data_layer table
INSERT into stock_data.raw_stock_data_layer(c, d, dp, h, l, o, pc, t, symbol,fetched_at,country)
  
SELECT
c,
d,
dp,
h,	
l,
o,
pc,
t,
symbol,
fetched_at,
country

FROM `red-function-478012-q6.stock_data.raw_stock_data_layer` as a --data comes from GCS bucket

-- WHERE Clause holds a deduplication logic thats comparing the Ticker name and its fetch data with existing rows in the table so it wont add already exisitng data from the cloud.
-- this also enables the table to hold historical data.

WHERE NOT EXISTS (
        SELECT 1 
        FROM `stock_data.raw_stock_data_layer` as b
        WHERE b.symbol = UPPER(TRIM(a.symbol)) AND b.fetched_at = CAST(a.fetched_at AS TIMESTAMP)
                 );

END;
