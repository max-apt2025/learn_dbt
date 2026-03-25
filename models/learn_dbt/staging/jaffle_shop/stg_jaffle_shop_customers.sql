{{ config(materialized='table') }}

with source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

transformed as (

    select
        id as customer_id,
        first_name ,
        last_name ,
        first_name ||' ' || last_name as full_name,
    {{ function('customer_email') }}(first_name,id) as customer_email

    from source

)

select * from transformed