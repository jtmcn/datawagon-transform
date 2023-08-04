with stg__longs_video_ads__channel_revenue__daily as (

    select *
    from {{ ref('stg__longs_video_ads__channel_revenue__daily') }}

),

stg__channels as (
    
    select 
        report_date_key,
        date_key,
        channel_id,
        channel_display_name
    from stg__longs_video_ads__channel_revenue__daily
    group by 
        report_date_key,
        date_key,
        channel_id,
        channel_display_name

)


select * 
from stg__channels