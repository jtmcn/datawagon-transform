{{ config(
    enabled=False,
) }}

with stg__longs_ads__channel_revenue as (

    select *
    from {{ ref('stg__longs_ads__channel_revenue') }}

),

stg__adj_claim_raw__channel_revenue as (

    select *
    from {{ ref('stg__adj_claim_raw__channel_revenue') }}

),

stg__subscription_music as (

    select *
    from {{ ref('stg__subscription_music') }}

),

stg__subscription_non_music as (

    select *
    from {{ ref('stg__subscription_non_music') }}

),

stg__channels as (

    select *
    from {{ ref('stg__channels') }}

),

stg__channel_revenue_with_name as (

    select 
        longs_ads.report_date_key,
        longs_ads.asset_channel_id,
        channels.channel_display_name,
        longs_ads.longs_ads__channel_revenue
    from stg__longs_ads__channel_revenue as longs_ads
    left join stg__channels as channels on
        longs_ads.report_date_key = channels.report_date_key
        and
        longs_ads.asset_channel_id = channels.channel_id

),

monthly_channel_revenue as (

    select 
        claim_raw.report_date_key,
        claim_raw.asset_channel_id,
        claim_raw.channel_display_name,
        claim_raw.longs__ads__partner_revenue,
        coalesce(adj_claim_raw.partner_revenue, 0.0) as longs__ads__partner_revenue_adj,
        coalesce(sub_music.partner_revenue, 0.0) as longs__subscription_music__partner_revenue,
        coalesce(sub_non_music.partner_revenue, 0.0) as longs__subscription_non_music__partner_revenue
    from stg__channel_revenue_with_name as claim_raw
    left join stg__adj_claim_raw__channel_revenue as adj_claim_raw
        on claim_raw.report_date_key = adj_claim_raw.report_date_key
        and
        claim_raw.asset_channel_id = adj_claim_raw.asset_channel_id
    left join stg__subscription_music as sub_music
        on claim_raw.report_date_key = sub_music.report_date_key
        and
        claim_raw.asset_channel_id = sub_music.asset_channel_id
    left join stg__subscription_non_music as sub_non_music
        on claim_raw.report_date_key = sub_non_music.report_date_key
        and
        claim_raw.asset_channel_id = sub_non_music.asset_channel_id

)


select * 
from monthly_channel_revenue