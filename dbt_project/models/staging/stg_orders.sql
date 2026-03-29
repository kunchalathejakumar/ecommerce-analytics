with source as (

    select * from {{ source('staging', 'orders') }}

),

renamed as (

    select
        order_id::varchar                                           as order_id,
        customer_id::varchar                                        as customer_id,
        coalesce(order_date, date_ordered)::date                    as order_date,
        case
            when lower(status) in ('complete', 'completed', 'done') then 'completed'
            else lower(status)
        end::varchar                                                as status,
        total_amount::numeric(12, 2)                                as total_amount,
        shipping_region::varchar                                    as shipping_region,
        current_timestamp                                           as loaded_at

    from source

)

select * from renamed
