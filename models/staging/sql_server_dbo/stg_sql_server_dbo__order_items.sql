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
        coalesce(_fivetran_deleted, false) as _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load

    from src_order_items

)

select * from renamed_casted
