-- staging models
with customers as (
select 
    *
from {{ ref('stg_raw__customers') }}
),
orders as (
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
-- CTE's or intermediate models--
order_items as (
select 
    i.order_id,
    i.price as price_amount,
    i.product_id,
    o.customer_id,
    o.order_status,
    p.product_category_name,
    p.product_weight_g
from {{ ref('stg_raw__orderitems') }} i
left join orders o
on i.order_id = o.order_id
left join products p
on i.product_id = p.product_id
),
payments as (
select
    order_id,
    payment_type,
    payment_amount
from {{ ref('stg_raw__payments') }}
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
    c.customer_city,
    c.customer_state,
    p.payment_type,
    p.payment_amount as payment_amount,
from order_items e
left join customers c
on e.customer_id = c.customer_id
left join payments p
on e.order_id = p.order_id
)

-- final model--
select
*
from customerorders