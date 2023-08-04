with stg__shorts__video__sub_rev as (

    select
        *
    from
        {{ ref('stg__shorts__video__sub_rev') }}

),
stg__shorts__video__sub_rev__daily_channel_revenue as (

    select
        report_date_key,
        date_key,
        channel_id,
        sum(partner_revenue) as channel_revenue
    from
        stg__shorts__video__sub_rev
    group by
        report_date_key,
        date_key,
        channel_id

)

select
    *
from
    stg__shorts__video__sub_rev__daily_channel_revenue
