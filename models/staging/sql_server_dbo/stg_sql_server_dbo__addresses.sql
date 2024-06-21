{{ 
    config(
      materialized='incremental'
    ) 
}}

with 

src_addresses as (

    select * from {{ source('sql_server_dbo', 'ADDRESSES') }}

),

renamed_casted  as (

    select
        address_id,
        zipcode,
        country,
        address,
        state,
        _fivetran_deleted,
        _fivetran_synced as date_load

    from src_addresses

)

select * from renamed_casted 
