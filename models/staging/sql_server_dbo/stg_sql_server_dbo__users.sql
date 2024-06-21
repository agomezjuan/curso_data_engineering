{{ 
    config(
      materialized='incremental'
    ) 
}}

with 

src_users as (

    select * from {{ source('sql_server_dbo', 'USERS') }}

),

renamed_casted as (

    select
        user_id,
        first_name,
        last_name,
        phone_number,
        email,
        ifnull(total_orders, 0) as total_orders,
        address_id,
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        coalesce(_fivetran_deleted, false) as _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load

    from src_users

)

select * from renamed_casted
