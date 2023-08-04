with stg__longs_ads__channel_revenue__daily as (

    select
        *
    from
        {{ ref('stg__longs_ads__channel_revenue__daily') }}

),
stg__claim_raw__channel_revenue as (

    select
        report_date_key,
        asset_channel_id,
        sum(channel_revenue) as channel_revenue
    from
        stg__longs_ads__channel_revenue__daily
    group by
        report_date_key,
        clean_channel_id

),
stg__longs_ads__channel_revenue as (

    select
        report_date_key,
        clean_channel_id as asset_channel_id,
        channel_revenue
    from
        stg__claim_raw__channel_revenue

)

select
    *
from
    stg__longs_ads__channel_revenue
