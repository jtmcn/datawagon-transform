with stg__longs__video__ad_rev as (

    select
        *
    from
        {{ ref('stg__longs__video__ad_rev') }}

),
stg__claim_raw__channel_revenue as (

    select
        report_date_key,
        date_key,
        {{ clean_channel_id('channel_id') }} as cleaned_channel_id,
        channel_display_name,
        sum(partner_revenue) as channel_revenue
    from
        stg__longs__video__ad_rev
    group by
        report_date_key,
        date_key,
        cleaned_channel_id,
        channel_display_name

),
stg__longs_ads__channel_revenue as (

    select
        report_date_key,
        date_key,
        cleaned_channel_id as channel_id,
        channel_display_name,
        channel_revenue
    from
        stg__claim_raw__channel_revenue

)

select
    *
from
    stg__longs_ads__channel_revenue
