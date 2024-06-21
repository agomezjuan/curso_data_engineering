{{
  config(
    materialized='view'
  )
}}

{% set order_statuses = obtener_valores(ref("stg_sql_server_dbo__orders"), 'status') %}

with src_orders as (

    select * from {{ source('sql_server_dbo', 'ORDERS') }}

),

renamed_casted as (

    select
        order_status_id,
        description,
        coalesce(_fivetran_deleted, false) as _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load

    from src_orders

)

select * from renamed_casted
