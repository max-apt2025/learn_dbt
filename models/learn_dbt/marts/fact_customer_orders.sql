with 
paid_orders as (
    select *
    from {{ ref("int_orders") }}

),

    final as (
        select
            order_id,
            customer_id,
            order_date,
            order_status,
            total_amount_paid,
            payment_finalized_date,
            customer_first_name,
            customer_last_name,

            -- sales transaction sequence
            row_number() over (order by order_id) as transaction_seq,

            -- customer sales sequence
            row_number() over (
                partition by customer_id order by order_id
            ) as customer_sales_seq,

            -- new vs returning customer
            case
                when
                    (
                        rank() over (
                            partition by customer_id order by order_date, order_id
                        )
                        = 1
                    )
                then 'new'
                else 'return'
            end as nvsr,

            --customer life time value
            sum(total_amount_paid) over (
                partition by customer_id order by order_date
            ) as customer_lifetime_value,

            --first order date
            first_value(order_date) over (
                partition by customer_id order by order_date
            ) as fdos,
            
        from paid_orders
        order by order_id
    )

select *
from final
