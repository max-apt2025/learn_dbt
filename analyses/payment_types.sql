--this is a sample sql for payments types sql--
with source as (
    select *
    from {{ ref('stg_stripe_payment') }}
    where payment_status = 'success'
),
paymt_amount as (
    select 
    paymentmethod,
    sum(payment_amount) as total_revenue,
    {% for paymentmethod in ['credit_card','bank_transfer','gift_card','coupon'] %}
        sum(case when paymentmethod = '{{paymentmethod}}' then payment_amount else 0 end ) as {{paymentmethod}}_amount{{',' if not loop.last else ''}}  
    {% endfor %}
    from source
    group by 1
)
select * from paymt_amount
