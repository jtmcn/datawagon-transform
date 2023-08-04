with src__raw_csv__adj_claim_raw as (

    select *
    from {{ ref('src__raw_csv__adj_claim_raw') }}

),

stg__adj_claim_raw__channel_revenue as (

    select 
        report_date_key,
        asset_channel_id,
        sum(owned_views) as owned_views,
        sum(partner_revenue) as partner_revenue
    from src__raw_csv__adj_claim_raw
    group by
        report_date_key,
        asset_channel_id

)


select * 
from stg__adj_claim_raw__channel_revenue