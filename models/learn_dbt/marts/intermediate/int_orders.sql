{{
    config(
        materialized='ephemeral'
    )
}}

-- import CTE's
with
    customers as (select * from {{ ref('stg_jaffle_shop_customers') }}),
    orders as (select * from {{ ref("stg_jaffle_shop_orders") }}),

    payments as (select * from {{ ref("stg_stripe_payment") }}),

    -- logical CTE's
    completed_payments as (
        select
            order_id,
            max(payment_created) as payment_finalized_date,
            sum(payment_amount) as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by 1
    ),
    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_date,
            orders.order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join completed_payments on orders.order_id = completed_payments.order_id
        left join customers on orders.customer_id = customers.customer_id

    )
    select * from paid_orders