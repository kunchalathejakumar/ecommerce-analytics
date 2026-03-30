{{
    config(
        materialized='view'
    )
}}

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

orders as (

    select * from {{ ref('int_orders') }}

),

joined as (

    select
        -- order item identifiers & core fields
        oi.order_id,
        oi.item_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.discount,
        oi.line_revenue,

        -- product attributes
        p.category      as product_category,
        p.cost_price    as product_cost_price,
        p.price         as product_price,
        p.margin        as product_margin,
        p.margin_pct    as product_margin_pct,

        -- order & customer context
        o.order_date,
        o.status       as order_status,
        o.customer_id,
        o.customer_name,
        o.customer_segment,
        o.customer_country,
        o.shipping_region,
        o.order_year,
        o.order_month,
        o.order_quarter,
        o.is_failed_order,

        -- derived cost & profit metrics
        CAST(oi.quantity * p.cost_price AS NUMERIC(12, 2))          as line_cost,

        CAST(
            oi.line_revenue - (oi.quantity * p.cost_price)
            AS NUMERIC(12, 2)
        )                                                           as line_profit,

        CASE
            WHEN oi.line_revenue = 0 OR oi.line_revenue IS NULL THEN NULL
            ELSE CAST(
                (oi.line_revenue - (oi.quantity * p.cost_price))
                / oi.line_revenue
                AS NUMERIC(8, 4)
            )
        END                                                         as line_margin_pct,
        oi.loaded_at
    from order_items oi
    left join products p
        on oi.product_id = p.product_id
    left join orders o
        on oi.order_id = o.order_id

)

select * from joined
