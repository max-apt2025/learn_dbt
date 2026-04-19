-- this is a analyses practive for numbers of orders daily--
with source as (
    select *
    from {{ ref('stg_jaffle_shop_orders') }}
),
daily_orders as (
    select
    order_date,
    count(*) as order_num,
    {% for order_status in ['return_pending','completed','returned'] %}
    sum(case when order_status = '{{order_status}}' then 1 else 0 end) as {{order_status}}_total{{',' if not loop.last else ''}}       
    {% endfor %}
    from source
    group by 1
),
previous_day as(
    select *,
    lag(order_num) over(order by order_date) as previous_day_orders
    from daily_orders
)
select * from previous_day

