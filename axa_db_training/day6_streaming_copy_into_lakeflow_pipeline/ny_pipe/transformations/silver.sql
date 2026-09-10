
create streaming table dev.naval_silver.sales_cleaned_pl 
(CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
as 
select distinct * except (_rescued_data, current_date, file_name) from stream dev.naval_bronze.sales_pl;



CREATE OR REFRESH STREAMING TABLE dev.naval_silver.products_cleaned_pl;

CREATE FLOW product_flow AS AUTO CDC INTO
  dev.naval_silver.products_cleaned_pl
FROM
  stream(dev.naval_bronze.products_pl)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum, _rescued_data, current_date,file_name)
STORED AS
  SCD TYPE 1;




CREATE OR REFRESH STREAMING TABLE dev.naval_silver.customers_cleaned_pl;

CREATE FLOW customer_flow AS AUTO CDC INTO
  dev.naval_silver.customers_cleaned_pl
FROM
  stream(dev.naval_bronze.customers_pl)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum, _rescued_data, current_date,file_name)
STORED AS
  SCD TYPE 2;

