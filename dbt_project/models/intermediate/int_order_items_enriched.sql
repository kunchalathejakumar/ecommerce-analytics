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

orders_enriched as (

    select * from {{ ref('int_orders_enriched') }}

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
        oe.order_date,
        oe.status       as order_status,
        oe.customer_id,
        oe.customer_name,
        oe.customer_segment,
        oe.customer_country,
        oe.shipping_region,
        oe.order_year,
        oe.order_month,
        oe.order_quarter,
        oe.is_failed_order,

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
    left join orders_enriched oe
        on oi.order_id = oe.order_id

)

select * from joined
