with 

source as (

    select * from {{ source('football_database', 'appearances') }}

),

renamed as (

    select
        gameid,
        playerid,
        goals,
        owngoals,
        shots,
        xgoals,
        xgoalschain,
        xgoalsbuildup,
        assists,
        keypasses,
        xassists,
        position,
        positionorder,
        yellowcard,
        redcard,
        time,
        substitutein,
        substituteout,
        leagueid

    from source

)

select * from renamed
