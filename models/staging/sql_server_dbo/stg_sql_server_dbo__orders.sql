{{
  config(
    materialized='view'
  )
}}

with 

src_orders as (

    select * from {{ source('sql_server_dbo', 'ORDERS') }}

),

renamed_casted as (

    select
        order_id,
        shipping_service,
        shipping_cost,
        address_id,
        created_at,
        promo_id,
        estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        delivered_at,
        tracking_id,
        status,
        coalesce(_fivetran_deleted, false) as _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load

    from src_orders

)

select * from renamed_casted
