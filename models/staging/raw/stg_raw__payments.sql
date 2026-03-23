with 

source as (

    select * from {{ source('raw', 'payments') }}

),

renamed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        round(payment_value) as payment_amount

    from source

)

select * from renamed