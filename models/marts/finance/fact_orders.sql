with customers as (
select 
    *
from {{ ref('stg_raw__customers') }}
),
order_items as (
    select *
    from {{ ref('orders_delivered') }}
),
customerorders as (
select
    e.order_id,
    e.product_id,
    e.customer_id,
    e.order_status,
    e.product_category_name,
    e.product_weight_g,
    e.price_amount,
    e.days_to_ship,
    c.customer_city,
    c.customer_state,
    e.payment_type,
    e.payment_amount,
from order_items e
left join customers c
on e.customer_id = c.customer_id
)
select
*
from customerorders 