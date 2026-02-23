with events as (
    select * from {{ ref('stg_events') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

enriched as (
    select
        e.event_id,
        e.session_id,
        e.user_id,
        e.event_type,
        e.created_at,
        e.traffic_source,
        e.uri,

        -- User info
        u.age,
        u.gender,
        u.country,
        u.city

    from events e
    left join users u on e.user_id = u.user_id
)

select * from enriched