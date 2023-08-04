with stg__longs__claim__ad_rev_adj as (

    select
        *
    from
        {{ ref('stg__longs__claim__ad_rev_adj') }}

),
stg__claim_raw__channel_revenue as (

    select
        report_date_key,
        {{ clean_channel_id('asset_channel_id') }} as clean_channel_id,
        sum(coalesce(claim_raw.partner_revenue, 0.0)) as longs_ads__channel_revenue_adj
    from
        stg__longs__claim__ad_rev_adj
    group by
        report_date_key,
        clean_channel_id

),
stg__longs_ads__channel_revenue_adj as (

    select
        report_date_key,
        clean_channel_id as asset_channel_id,
        longs_ads__channel_revenue_adj
    from
        stg__claim_raw__channel_revenue

)

select
    *
from
    stg__longs_ads__channel_revenue_adj
