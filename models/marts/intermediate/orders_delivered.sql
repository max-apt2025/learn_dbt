with orders as (
select 
    *
from {{ ref('stg_raw__orders') }}
where order_status = 'delivered'
),
products as (
select 
    *
from {{ ref('stg_raw__products') }}
),
payments as (
select
    order_id,
    payment_type,
    payment_amount
from {{ ref('stg_raw__payments') }}
),
order_items as (
select 
    i.order_id,
    i.price as price_amount,
    i.product_id,
    o.customer_id,
    o.order_status,
    o.days_to_ship,
    p.product_category_name,
    p.product_weight_g,
    py.payment_type,
    py.payment_amount
from {{ ref('stg_raw__orderitems') }} i
left join orders o
on i.order_id = o.order_id
left join products p
on i.product_id = p.product_id
left join payments py
on i.order_id = py.order_id
)
Select * 
from order_items

