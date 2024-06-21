{{ 
    config(
      materialized='incremental'
    ) 
}}

with 

src_promos as (

    select * from {{ source('sql_server_dbo', 'PROMOS') }}

),

renamed_casted as (

    select
        distinct case when status = 'active' then 1
            else 0
        end as status_id,
        status as description

    from src_promos

)

select * from renamed_casted
