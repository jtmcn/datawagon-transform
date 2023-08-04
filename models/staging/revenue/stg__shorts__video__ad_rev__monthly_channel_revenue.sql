with stg__shorts__video__ad_rev as (

    select
        *
    from
        {{ ref('stg__shorts__video__ad_rev') }}

),
stg__shorts__video__ad_rev__daily_channel_revenue as (

    select
        report_date_key,
        channel_id,
        sum(partner_revenue) as channel_revenue
    from
        stg__shorts__video__ad_rev
    group by
        report_date_key,
        channel_id

)

select
    *
from
    stg__shorts__video__ad_rev__daily_channel_revenue
