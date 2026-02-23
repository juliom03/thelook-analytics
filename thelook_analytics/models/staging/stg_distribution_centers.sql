with source as (
    select * from {{ source('thelook', 'distribution_centers') }}
),
renamed as (
    select
        id as distribution_center_id,
        name as distribution_center_name,
        latitude,
        longitude,
        distribution_center_geom
    from source
)
select * from renamed