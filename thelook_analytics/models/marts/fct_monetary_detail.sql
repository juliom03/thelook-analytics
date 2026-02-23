with base as (
    select * from {{ ref('int_order_items_enriched') }}
),

final as (
    select
        -- Keys
        order_item_id,
        order_id,
        user_id,
        product_id,
        created_at,

        -- Product dimensions
        product_name,
        category,
        brand,
        department,

        -- User dimensions
        gender,
        country,
        city,
        traffic_source,
        CASE 
            WHEN age < 18              THEN '<18'
            WHEN age BETWEEN 18 AND 29 THEN '18-29'
            WHEN age BETWEEN 30 AND 45 THEN '30-45'
            WHEN age BETWEEN 46 AND 59 THEN '46-59'
            WHEN age >= 60             THEN '60+'
        END as age_group,

        -- Order status
        status,
        shipped_at,
        delivered_at,
        returned_at,
        days_to_deliver,
        days_to_return,

        -- Metrics
        sale_price,
        retail_price,
        cost,
        gross_margin

    from base
)

select * from final