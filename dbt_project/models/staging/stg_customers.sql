with source as (

    select * from {{ source('staging', 'customers') }}

),

renamed as (

    select
        customer_id::varchar          as customer_id,
        initcap(name)::varchar        as name,
        lower(email)::varchar         as email,
        lower(segment)::varchar       as segment,
        upper(country)::varchar       as country,
        current_timestamp             as loaded_at

    from source
    

)

select * from renamed
