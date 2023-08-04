with src__raw_csv__red_music_rawdata_asset as (

    select *
    from {{ ref('src__raw_csv__red_music_rawdata_asset') }}

),

stg__subscription_music as (

    select 
        report_date_key,
        asset_channel_id,
        sum(owned_views) as owned_views,
        sum(partner_revenue) as partner_revenue
    from src__raw_csv__red_music_rawdata_asset
    group by
        report_date_key,
        asset_channel_id

)


select * 
from stg__subscription_music