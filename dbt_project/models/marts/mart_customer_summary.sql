{{ config(
    materialized='table',
    indexes=[
      {'columns': ['customer_id']},
      {'columns': ['customer_segment']},
      {'columns': ['lifetime_value']}
    ]
) }}

with orders as (
    select *
    from {{ ref('int_orders') }}
    where not is_failed_order
),

order_items as (
    select *
    from {{ ref('int_order_items') }}
    where not is_failed_order
)

select
    o.customer_id,
    max(o.customer_name) as customer_name,
    max(o.customer_email) as customer_email,
    max(o.customer_segment) as customer_segment,
    max(o.customer_country) as customer_country,
    count(distinct o.order_id) as total_orders,
    sum(o.total_amount) as lifetime_value,
    avg(o.total_amount) as avg_order_value,
    max(o.total_amount) as max_order_value,
    count(distinct oi.product_id) as unique_products_purchased,
    sum(oi.quantity) as total_items_purchased,
    avg(oi.quantity) as avg_items_per_order,
    sum(oi.line_profit) as total_profit,
    avg(oi.line_profit) as avg_profit_per_item,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    (max(o.order_date) - min(o.order_date)) as customer_lifespan_days,
    (current_date - max(o.order_date)) as days_since_last_order,
    case
        when sum(o.total_amount) > 10000 then 'vip'
        when sum(o.total_amount) > 5000 then 'high'
        when sum(o.total_amount) > 1000 then 'medium'
        else 'low'
    end as customer_value_tier,
    current_timestamp as dbt_updated_at
from orders o
left join order_items oi
    on o.order_id = oi.order_id
group by o.customer_id
order by lifetime_value desc
