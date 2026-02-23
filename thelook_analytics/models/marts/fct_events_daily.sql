with base as (
    select * from {{ ref('int_events_funnel') }}
),

final as (
    select
        DATE(created_at)    as event_date,
        event_type,
        traffic_source,
        country,
        gender,

        COUNT(event_id)                                          as total_events,
        COUNT(DISTINCT session_id)                               as total_sessions,
        COUNT(DISTINCT user_id)                                  as total_users,
        COUNTIF(event_type = 'purchase')                         as total_purchases,
        COUNTIF(event_type = 'cart')                             as total_add_to_cart,
        COUNTIF(event_type = 'cancel')                           as total_cancels,
        SAFE_DIVIDE(
            COUNTIF(event_type = 'purchase'),
            COUNTIF(event_type = 'cart')
        )                                                        as cart_to_purchase_rate

    from base
    group by 1,2,3,4,5
)

select * from final