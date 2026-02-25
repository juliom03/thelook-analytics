
--Only for learnign porpuses, we will do it incremental

{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

enriched as (
    select
        -- Order item keys
        oi.order_item_id,
        oi.order_id,
        oi.user_id,
        oi.product_id,

        -- Order item metrics
        oi.status,
        oi.sale_price,
        oi.created_at,
        oi.shipped_at,
        oi.delivered_at,
        oi.returned_at,

        -- Delivery time calculations
        TIMESTAMP_DIFF(oi.delivered_at, oi.shipped_at, DAY)   as days_to_deliver,
        TIMESTAMP_DIFF(oi.returned_at, oi.delivered_at, DAY)  as days_to_return,

        -- From orders
        o.num_of_item,

        -- From products
        p.product_name,
        p.category,
        p.brand,
        p.department,
        p.retail_price,
        p.cost,
        oi.sale_price - p.cost                                as gross_margin,

        -- From users
        u.email,
        u.age,
        u.gender,
        u.country,
        u.city,
        u.traffic_source

    from order_items oi
    left join orders o   on oi.order_id   = o.order_id
    left join products p on oi.product_id = p.product_id
    left join users u    on oi.user_id    = u.user_id
)

{% if is_incremental() %}
WHERE oi.created_at >= (SELECT MAX(created_at) FROM {{ this }})
{% endif %}

select * from enriched