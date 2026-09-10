--pipeline: streaming tables, materialized view, temp views
--medallion 
--streaming   Streaming    Materialsied view 
--bronze      silver        gold

create streaming table dev.naval_bronze.sales_pl as
select *,current_date() as current_date, _metadata.file_name  from stream read_files("/Volumes/dev/naval/raw/months/");


create streaming table dev.naval_bronze.products_pl as
select *,current_date() as current_date, _metadata.file_name  from stream read_files("/Volumes/dev/naval/raw/products/");



create streaming table dev.naval_bronze.customers_pl as
select *,current_date() as current_date, _metadata.file_name  from stream read_files("/Volumes/dev/naval/raw/customers/");