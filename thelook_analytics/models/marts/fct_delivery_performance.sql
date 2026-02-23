with base as (
    select * from {{ ref('int_order_items_enriched') }}
),

final as (
    select
        -- Dimensions
        DATE(created_at)        as order_date,
        category,
        department,
        country,
        city,
        status,

        -- Volume
        COUNT(order_item_id)                                as total_items,

        -- Delivery metrics
        AVG(days_to_deliver)                                as avg_days_to_deliver,
        MIN(days_to_deliver)                                as min_days_to_deliver,
        MAX(days_to_deliver)                                as max_days_to_deliver,

        -- Return metrics
        AVG(days_to_return)                                 as avg_days_to_return,
        COUNTIF(returned_at IS NOT NULL)                    as total_returns,
        SAFE_DIVIDE(
            COUNTIF(returned_at IS NOT NULL),
            COUNT(order_item_id)
        )                                                   as return_rate,

        -- Financial impact of returns
        AVG(CASE WHEN returned_at IS NOT NULL 
            THEN sale_price END)                            as avg_price_returned_items,
        AVG(CASE WHEN returned_at IS NULL 
            THEN sale_price END)                            as avg_price_kept_items

    from base
    where status != 'processing'
    group by 1,2,3,4,5,6
)

select * from final