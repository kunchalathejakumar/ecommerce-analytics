{{ config(
    materialized='table',
    indexes=[
      {'columns': ['order_date']},
      {'columns': ['shipping_region']},
      {'columns': ['customer_segment']}
    ]
) }}

with orders as (
    select *
    from {{ ref('int_orders') }}
    where not is_failed_order
)

select
    order_date,
    order_year,
    order_month,
    order_quarter,
    order_year_month,
    order_day_of_week,
    order_day_name,
    shipping_region,
    customer_segment,
    count(distinct order_id) as order_count,
    count(distinct customer_id) as unique_customers,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value,
    min(total_amount) as min_order_value,
    max(total_amount) as max_order_value,
    percentile_cont(0.5) within group (order by total_amount) as median_order_value,
    sum(case when order_value_tier = 'high' then 1 else 0 end) as high_value_orders,
    sum(case when order_value_tier = 'medium' then 1 else 0 end) as medium_value_orders,
    sum(case when order_value_tier = 'low' then 1 else 0 end) as low_value_orders,
    current_timestamp as updated_at
from orders
group by
    order_date,
    order_year,
    order_month,
    order_quarter,
    order_year_month,
    order_day_of_week,
    order_day_name,
    shipping_region,
    customer_segment
order by order_date desc
