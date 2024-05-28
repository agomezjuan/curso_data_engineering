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
        md5(promo_id) as promo_id,
        promo_id as promo_name,
        discount as discount_euro,
        case when status = 'active' then 1
            else 0
        end as status_id,
        coalesce(_fivetran_deleted, false) as _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load

    from src_promos
    union all
    select
        distinct md5('unknown'),
        'unknown',
        0,
        0,
        false,
        convert_timezone('UTC', _fivetran_synced)
    from src_promos

)

select * from renamed_casted
