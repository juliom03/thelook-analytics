with source as (
    select * from {{ source('thelook', 'products') }}
),
renamed as (
    select
        id as product_id,
        name as product_name,
        category,
        brand,
        department,
        retail_price,
        cost
    from source
)
select * from renamed