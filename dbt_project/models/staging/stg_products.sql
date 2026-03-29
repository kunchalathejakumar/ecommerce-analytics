with source as (

    select * from {{ source('staging', 'products') }}

),

renamed as (

    select
        product_id::varchar                                                                     as product_id,
        initcap(name)::varchar                                                                  as product_name,
        lower(category)::varchar                                                                as category,
        list_price::numeric(10, 2)                                                              as price,
        cost_price::numeric(10, 2)                                                              as cost_price,
        (list_price::numeric(10, 2) - cost_price::numeric(10, 2))                                 as margin,
        round(
            ((list_price::numeric(10, 2) - cost_price::numeric(10, 2))
                / nullif(cost_price::numeric(10, 2), 0)) * 100,
            2
        )                                                                                       as margin_pct,
        current_timestamp                                                                       as loaded_at

    from source

    where
        cost_price::numeric(10, 2) > 0
        and list_price::numeric(10, 2) >= cost_price::numeric(10, 2)

)

select * from renamed
