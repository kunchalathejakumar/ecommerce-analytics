{{ config(
    materialized='table',
    indexes=[
      {'columns': ['product_id']},
      {'columns': ['product_category']},
      {'columns': ['total_revenue']}
    ]
) }}

with order_items as (
    select *
    from {{ ref('int_order_items') }}
    where not is_failed_order
),

product_aggregates as (
    select
        product_id,
        max(product_name) as product_name,
        max(product_category) as product_category,
        max(product_cost) as product_cost,
        max(product_price) as product_price,
        max(product_margin_pct) as product_margin_pct,
        count(distinct order_id) as orders_with_product,
        sum(quantity) as total_quantity_sold,
        sum(line_revenue) as total_revenue,
        avg(unit_price) as avg_selling_price,
        sum(line_cost) as total_cost,
        sum(line_profit) as total_profit,
        avg(line_margin_pct) as avg_margin_pct,
        avg(discount) as avg_discount_rate,
        sum(case when discount > 0 then 1 else 0 end) as discounted_sales_count,
        count(*) as total_sales_count,
        sum(case when customer_segment = 'premium' then quantity else 0 end) as qty_sold_premium,
        sum(case when customer_segment = 'standard' then quantity else 0 end) as qty_sold_standard,
        sum(case when customer_segment = 'basic' then quantity else 0 end) as qty_sold_basic
    from order_items
    group by product_id
)

select
    product_id,
    product_name,
    product_category,
    product_cost,
    product_price,
    product_margin_pct,
    orders_with_product,
    total_quantity_sold,
    total_revenue,
    avg_selling_price,
    total_cost,
    total_profit,
    avg_margin_pct,
    avg_discount_rate,
    discounted_sales_count,
    total_sales_count,
    round((discounted_sales_count::numeric / nullif(total_sales_count, 0)) * 100, 2) as discount_rate_pct,
    qty_sold_premium,
    qty_sold_standard,
    qty_sold_basic,
    rank() over (order by total_revenue desc) as revenue_rank,
    rank() over (order by total_profit desc) as profit_rank,
    current_timestamp as dbt_updated_at
from product_aggregates
order by total_revenue desc
