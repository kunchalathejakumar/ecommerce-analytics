{{
    config(
        materialized='view'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

joined as (

    select
        -- order identifiers & core fields
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.total_amount,
        o.shipping_region,

        -- customer attributes
        c.name           as customer_name,
        c.email          as customer_email,
        c.segment        as customer_segment,
        c.country        as customer_country,

        -- time dimensions
        EXTRACT(YEAR    FROM o.order_date)::int                    as order_year,
        EXTRACT(MONTH   FROM o.order_date)::int                    as order_month,
        EXTRACT(QUARTER FROM o.order_date)::int                    as order_quarter,
        EXTRACT(DOW     FROM o.order_date)::int                    as order_day_of_week,
        TO_CHAR(o.order_date, 'YYYY-MM')                           as order_year_month,
        TO_CHAR(o.order_date, 'Day')                               as order_day_name,

        -- business logic flags
        CASE
            WHEN o.status IN ('cancelled', 'returned') THEN TRUE
            ELSE FALSE
        END                                                         as is_failed_order,

        -- order value tier
        CASE
            WHEN o.total_amount > 1000 THEN 'high'
            WHEN o.total_amount > 500  THEN 'medium'
            ELSE 'low'
        END                                                         as order_value_tier,

        o.loaded_at

    from orders o
    left join customers c
        on o.customer_id = c.customer_id

)

select * from joined
