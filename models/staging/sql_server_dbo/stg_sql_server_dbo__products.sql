{{ 
    config(
      materialized='incremental'
    ) 
}}

with 

src_products as (

    select * from {{ source('sql_server_dbo', 'PRODUCTS') }}

),

renamed_casted as (

    select
        product_id,
        price as price_euro,
        name,
        inventory,
        coalesce(_fivetran_deleted, false) as _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load

    from src_products

)

select * from renamed_casted
