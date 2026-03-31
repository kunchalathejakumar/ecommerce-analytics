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

customers as (
    select
        customer_id,
        name as customer_name,
        email as customer_email,
        segment as customer_segment,
        country as customer_country
    from {{ ref('stg_customers') }}
),

order_items as (
    select *
    from {{ ref('int_order_items') }}
    where not is_failed_order
),

customer_orders as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(total_amount) as lifetime_value,
        avg(total_amount) as avg_order_value,
        max(total_amount) as max_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by customer_id
),

customer_item_metrics as (
    select
        o.customer_id,
        count(distinct oi.product_id) as unique_products_purchased,
        sum(oi.quantity) as total_items_purchased,
        avg(oi.line_profit) as avg_profit_per_item,
        sum(oi.line_profit) as total_profit
    from orders o
    left join order_items oi
        on o.order_id = oi.order_id
    group by o.customer_id
)

select
    co.customer_id,
    c.customer_name,
    c.customer_email,
    c.customer_segment,
    c.customer_country,
    co.total_orders,
    co.lifetime_value,
    co.avg_order_value,
    co.max_order_value,
    coalesce(cim.unique_products_purchased, 0) as unique_products_purchased,
    coalesce(cim.total_items_purchased, 0) as total_items_purchased,
    round(
        coalesce(cim.total_items_purchased, 0)::numeric / nullif(co.total_orders, 0),
        2
    ) as avg_items_per_order,
    coalesce(cim.total_profit, 0) as total_profit,
    cim.avg_profit_per_item,
    co.first_order_date,
    co.last_order_date,
    (co.last_order_date - co.first_order_date) as customer_lifespan_days,
    (current_date - co.last_order_date) as days_since_last_order,
    case
        when co.lifetime_value > 10000 then 'vip'
        when co.lifetime_value > 5000 then 'high'
        when co.lifetime_value > 1000 then 'medium'
        else 'low'
    end as customer_value_tier,
    current_timestamp as dbt_updated_at
from customer_orders co
left join customers c
    on co.customer_id = c.customer_id
left join customer_item_metrics cim
    on co.customer_id = cim.customer_id
order by co.lifetime_value desc
