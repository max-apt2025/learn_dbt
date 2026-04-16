-- one off analyses to calculate the total revenue from time to time
with payments as (
    select * from {{ ref('stg_stripe_payment') }}
    where payment_status= 'success'
),
revenue as(
    select 
        sum(payment_amount) as revenue,
    from payments

)
Select * from revenue