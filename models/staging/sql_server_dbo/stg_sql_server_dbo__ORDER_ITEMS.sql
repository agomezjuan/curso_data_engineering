{{
  config(
    materialized='view'
  )
}}

with 

src_order_items as (

    select * from {{ source('sql_server_dbo', 'ORDER_ITEMS') }}

),

renamed_casted as (

    select
        order_id,
        product_id,
        quantity,
        _fivetran_deleted,
        _fivetran_synced as date_load

    from src_order_items

)

select * from renamed_casted
