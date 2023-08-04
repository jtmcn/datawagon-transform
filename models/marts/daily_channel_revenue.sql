{{ config(
    enabled=False,
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
stg__longs__claim__ad_rev__daily_channel_revenue as (

    select 
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as longs__ad_revenue
    from {{ ref('stg__longs__claim__ad_rev__daily_channel_revenue') }}

),
stg__channel_revenue_with_name as (

    select 
        longs_ad_rev.report_date_key,
        longs_ad_rev.date_key,
        longs_ad_rev.asset_channel_id,
        channels.channel_display_name,
        longs_ad_rev.longs__ad_revenue
    from stg__channels as channels
    full outer join stg__longs__claim__ad_rev__daily_channel_revenue as longs_ad_rev on
        longs_ad_rev.date_key = channels.date_key
        and
        longs_ad_rev.asset_channel_id = channels.channel_id

),
stg__longs__music_asset__sub_rev__daily_channel_revenue as (

    select
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as longs__music_sub_revenue
    from
        {{ ref('stg__longs__music_asset__sub_rev__daily_channel_revenue') }}

),
stg__channel_revenue__music_sub as (

    select
        channel_revenue.report_date_key,
        channel_revenue.date_key,
        channel_revenue.asset_channel_id,
        channel_revenue.channel_display_name,
        channel_revenue.longs__ad_revenue,
        music_sub.longs__music_sub_revenue

    from stg__channel_revenue_with_name as channel_revenue
    left join stg__longs__music_asset__sub_rev__daily_channel_revenue as music_sub on
        music_sub.date_key = channel_revenue.date_key
        and
        music_sub.asset_channel_id = channel_revenue.asset_channel_id

),
stg__longs__asset__sub_rev__daily_channel_revenue as (

    select
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as longs__non_music_sub_revenue
    from
        {{ ref('stg__longs__asset__sub_rev__daily_channel_revenue') }}

),
stg__channel_revenue__music_sub__non_music_sub as (

    select
        channel_revenue.report_date_key,
        channel_revenue.date_key,
        channel_revenue.asset_channel_id,
        channel_revenue.channel_display_name,
        channel_revenue.longs__ad_revenue,
        channel_revenue.longs__music_sub_revenue,
        non_music_sub.longs__non_music_sub_revenue

    from stg__channel_revenue__music_sub as channel_revenue
    left join stg__longs__asset__sub_rev__daily_channel_revenue as non_music_sub on
        non_music_sub.date_key = channel_revenue.date_key
        and
        non_music_sub.asset_channel_id = channel_revenue.asset_channel_id

),
stg__shorts__video__ad_rev__daily_channel_revenue as (

    select
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as shorts__ad_revenue
    from
        {{ ref('stg__shorts__video__ad_rev__daily_channel_revenue') }}

),

stg__shorts__video__sub_rev__daily_channel_revenue as (

    select
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as shorts__sub_revenue
    from
        {{ ref('stg__shorts__video__sub_rev__daily_channel_revenue') }}

),
stg__channel_revenue__music_sub__non_music_sub__shorts as (

    select
        channel_revenue.report_date_key,
        channel_revenue.date_key,
        channel_revenue.asset_channel_id,
        channel_revenue.channel_display_name,
        channel_revenue.longs__ad_revenue,
        channel_revenue.longs__music_sub_revenue,
        channel_revenue.longs__non_music_sub_revenue,
        shorts_ad.longs__ad_revenue as shorts__ad_revenue,
        shorts_sub.longs__sub_revenue as shorts__sub_revenue

    from stg__channel_revenue__music_sub__non_music_sub as channel_revenue
    left join stg__shorts__video__ad_rev__daily_channel_revenue as shorts_ad on
        shorts_ad.date_key = channel_revenue.date_key
        and
        shorts_ad.asset_channel_id = channel_revenue.asset_channel_id
    left join stg__shorts__video__sub_rev__daily_channel_revenue as shorts_sub on
        shorts_sub.date_key = channel_revenue.date_key
        and
        shorts_sub.asset_channel_id = channel_revenue.asset_channel_id

),

stg__longs__claim__ad_rev__daily_channel_revenue_adj as (

    select 
        report_date_key,
        date_key,
        asset_channel_id,
        channel_revenue as longs__ad_revenue_adj
    from {{ ref('stg__longs__claim__ad_rev__daily_channel_revenue_adj') }}

),

stg__channel_revenue_adj as (

    select
        channel_revenue.report_date_key,
        channel_revenue.date_key,
        channel_revenue.asset_channel_id,
        channel_revenue.channel_display_name,
        channel_revenue.longs__ad_revenue,
        channel_revenue.longs__music_sub_revenue,
        channel_revenue.longs__non_music_sub_revenue,
        channel_revenue.shorts__ad_revenue,
        channel_revenue.shorts__sub_revenue,
        longs_adj.longs__ad_revenue_adj

    from stg__channel_revenue__music_sub__non_music_sub__shorts as channel_revenue
    left join stg__longs__claim__ad_rev__daily_channel_revenue_adj as longs_adj on
        longs_adj.date_key = channel_revenue.date_key
        and
        longs_adj.asset_channel_id = channel_revenue.asset_channel_id

),
daily_channel_revenue as (


    select 
        report_date_key,
        date_key,
        asset_channel_id,
        channel_display_name,
        longs__ad_revenue,
        longs__ad_revenue_adj,
        longs__music_sub_revenue,
        longs__non_music_sub_revenue,
        shorts__ad_revenue,
        shorts__sub_revenue

    from stg__channel_revenue_adj

)


select *
from daily_channel_revenue
