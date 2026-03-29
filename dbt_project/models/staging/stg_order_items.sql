with source as (

    select * from {{ source('staging', 'order_items') }}

),

renamed as (

    select
        order_id::varchar                                               as order_id,
        item_id::int                                                    as item_id,
        product_id::varchar                                             as product_id,
        quantity::int                                                   as quantity,
        unit_price::numeric(10, 2)                                      as unit_price,
        discount::numeric(5, 4)                                         as discount,
        (quantity::int * unit_price::numeric(10, 2) * (1 - coalesce(discount::numeric(5, 4), 0)))::numeric(12, 2)  as line_revenue,
        current_timestamp                                               as loaded_at

    from source

    where
        quantity::int > 0
        and unit_price::numeric(10, 2) > 0

)

select * from renamed
