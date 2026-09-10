CREATE OR REFRESH MATERIALIZED VIEW dev.naval_gold.sales_product_insights
COMMENT 'Gold layer: sales insights by product category'
AS
SELECT
  p.product_category,
  p.product_name,
  COUNT(s.order_id)        AS total_orders,
  SUM(s.quantity)           AS total_quantity_sold,
  ROUND(SUM(s.total_amount), 2)   AS total_revenue,
  ROUND(AVG(s.total_amount), 2)   AS avg_order_value,
  ROUND(SUM(s.discount_amount), 2) AS total_discounts,
  ROUND(SUM(s.total_amount) - SUM(s.discount_amount), 2) AS net_revenue,
  MIN(s.order_date)         AS first_order_date,
  MAX(s.order_date)         AS last_order_date
FROM dev.naval_silver.sales_cleaned_pl s
JOIN dev.naval_silver.products_cleaned_pl p
  ON s.product_id = p.product_id
GROUP BY ALL;

