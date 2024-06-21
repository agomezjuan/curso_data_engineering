{{
  config(
    materialized='view'
  )
}}

with src_budget as (

    select * from {{ source('google_sheets', 'budget') }}

),

renamed_casted as (

    select
        _row,
        quantity,
        month as date,
        monthname(month) as month,
        product_id,
        null as _fivetran_deleted,
        _fivetran_synced as date_load

    from src_budget

)

select * from renamed_casted
