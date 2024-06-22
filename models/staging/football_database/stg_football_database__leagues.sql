with 

source as (

    select * from {{ source('football_database', 'leagues') }}

),

renamed as (

    select
        leagueid,
        name,
        understatnotation

    from source

)

select * from renamed
