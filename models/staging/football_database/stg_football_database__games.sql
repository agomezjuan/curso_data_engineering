{{ config(
    materialized='incremental',
    unique_key='gameid',
    post_hook='ANALYZE'
) }}


with 

source as (

    select * from {{ source('football_database', 'games') }}

),

incremental_source as (
    {% if is_incremental() %}
    select * from source
    where cast(gameid as int) > (select coalesce(max(gameid), 0) from {{ this }})
    {% else %}
    select * from source
    {% endif %}
),

renamed as (
    select
        cast(gameid as int) as gameid,
        cast(leagueid as int) as leagueid,
        season,
        date,
        hometeamid,
        awayteamid,
        homegoals,
        awaygoals,
        homeprobability,
        drawprobability,
        awayprobability,
        homegoalshalftime,
        awaygoalshalftime,
        cast(b365h as float) as b365h,
        cast(b365d as float) as b365d,
        cast(b365a as float) as b365a,
        cast(bwh as float) as bwh,
        cast(bwd as float) as bwd,
        cast(bwa as float) as bwa,
        cast(iwh as float) as iwh,
        cast(iwd as float) as iwd,
        cast(iwa as float) as iwa,
        cast(psh as float) as psh,
        cast(psd as float) as psd,
        cast(psa as float) as psa,
        cast(whh as float) as whh,
        cast(whd as float) as whd,
        cast(wha as float) as wha,
        cast(vch as float) as vch,
        cast(vcd as float) as vcd,
        cast(vca as float) as vca,
        cast(psch as float) as psch,
        cast(pscd as float) as pscd,
        cast(psca as float) as psca
    from source
),

select * from renamed
