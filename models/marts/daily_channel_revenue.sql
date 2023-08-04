{{ config(
    unique_key='date_key, asset_channel_id',
) }}

with stg__channels as (

    select 
        report_date_key,
        date_key,
        channel_id,
        channel_display_name
    from {{ ref('stg__channel_id_on_date') }}

),
stg__longs_claims_ads__channel_revenue__daily as (

    select 
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as longs_ad_rev__channel_revenue
    from {{ ref('stg__longs_claims_ads__channel_revenue__daily') }}

),
stg__channel_revenue_with_name as (

    select 
        longs_ad_rev.report_date_key,
        longs_ad_rev.date_key,
        longs_ad_rev.asset_channel_id,
        channels.channel_display_name,
        longs_ad_rev.longs_ad_rev__channel_revenue
    from stg__channels as channels
    full outer join stg__longs_claims_ads__channel_revenue__daily as longs_ad_rev on
        longs_ad_rev.date_key = channels.date_key
        and
        longs_ad_rev.asset_channel_id = channels.channel_id

)

select *
from stg__channel_revenue_with_name
