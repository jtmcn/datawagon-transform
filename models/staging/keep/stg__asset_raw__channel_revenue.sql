with src__raw_csv__asset_raw as (

    select *
    from {{ ref('stg__longs__asset__ad_rev') }}

),

stg__asset_raw__channel_revenue as (

    select 
        report_date_key,
        asset_channel_id,
        sum(owned_views) as owned_views,
        sum(partner_revenue) as partner_revenue
    from src__raw_csv__asset_raw
    group by
        report_date_key,
        asset_channel_id

)


select * 
from stg__asset_raw__channel_revenue