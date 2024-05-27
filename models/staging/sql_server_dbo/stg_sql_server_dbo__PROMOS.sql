{{
  config(
    materialized='view'
  )
}}

with 

src_promos as (

    select * from {{ source('sql_server_dbo', 'PROMOS') }}

),

renamed_casted as (

    select
        promo_id,
        discount,
        status,
        _fivetran_deleted,
        _fivetran_synced as date_load

    from src_promos

)

select * from renamed_casted