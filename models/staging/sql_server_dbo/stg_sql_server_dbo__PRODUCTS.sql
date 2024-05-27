{{
  config(
    materialized='view'
  )
}}

with 

src_products as (

    select * from {{ source('sql_server_dbo', 'PRODUCTS') }}

),

renamed_casted as (

    select
        product_id,
        price,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced

    from src_products

)

select * from renamed_casted
