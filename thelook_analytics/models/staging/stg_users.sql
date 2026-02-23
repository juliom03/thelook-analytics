with source as (
    select * from {{ source('thelook', 'users') }}
),
renamed as (
    select
        id as user_id,
        first_name,
        last_name,
        email,
        age,
        gender,
        country,
        city,
        created_at,
        traffic_source
    from source
)
select * from renamed