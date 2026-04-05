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

product_sales as (
    select
        product_id,
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
        sum(case when lower(customer_segment) = 'enterprise' then quantity else 0 end) as qty_sold_enterprise,
        sum(case when lower(customer_segment) = 'online' then quantity else 0 end) as qty_sold_online,
        sum(case when lower(customer_segment) = 'retail' then quantity else 0 end) as qty_sold_retail,
        sum(case when lower(customer_segment) = 'unknown' then quantity else 0 end) as qty_sold_unknown,
        sum(case when lower(customer_segment) = 'wholesale' then quantity else 0 end) as qty_sold_wholesale
    from order_items
    group by product_id
),

products as (
    select
        product_id,
        product_name,
        category as product_category,
        cost_price as product_cost,
        price as product_price,
        margin_pct as product_margin_pct
    from {{ ref('stg_products') }}
)

select
    s.product_id,
    p.product_name,
    coalesce(p.product_category, 'unknown') as product_category,
    p.product_cost,
    p.product_price,
    p.product_margin_pct,
    s.orders_with_product,
    s.total_quantity_sold,
    s.total_revenue,
    s.avg_selling_price,
    s.total_cost,
    s.total_profit,
    s.avg_margin_pct,
    s.avg_discount_rate,
    s.discounted_sales_count,
    s.total_sales_count,
    round((s.discounted_sales_count::numeric / nullif(s.total_sales_count, 0)) * 100, 2) as discount_rate_pct,
    s.qty_sold_enterprise,
    s.qty_sold_online,
    s.qty_sold_retail,
    s.qty_sold_unknown,
    s.qty_sold_wholesale,
    rank() over (order by s.total_revenue desc) as revenue_rank,
    rank() over (order by s.total_profit desc) as profit_rank,
    current_timestamp as dbt_updated_at
from product_sales s
left join products p
    on s.product_id = p.product_id
order by s.total_revenue desc
