with stg__longs__video__ad_rev__daily_channel_revenue as (

    select *
    from {{ ref('stg__longs__video__ad_rev__daily_channel_revenue') }}

),

stg__channels as (
    
    select 
        report_date_key,
        date_key,
        channel_id,
        channel_display_name
    from stg__longs__video__ad_rev__daily_channel_revenue
    group by 
        report_date_key,
        date_key,
        channel_id,
        channel_display_name

)


select * 
from stg__channels