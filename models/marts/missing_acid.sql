with longs__asset__ad_missing_asset_id as (

    select
        report_date_key,
        date_key,
        asset_id,
        video_id,
        channel_id
    from
        {{ ref('stg__longs__asset__ad_rev') }}
    where
        asset_channel_id is null
    group by
        report_date_key,
        date_key,
        asset_id,
        video_id,
        channel_id

),

missing_asset_id__channel_name as (

    select
        ads.report_date_key,
        ads.date_key,
        ads.asset_id,
        ads.video_id,
        ads.channel_id,
        cid.channel_display_name
    from longs__asset__ad_missing_asset_id as ads
    left join int__channel_id_on_date as cid
        on ads.channel_id = cid.channel_id
            and ads.date_key = cid.date_key


)

{# TODO: add asset and video titles #}
select *
from missing_asset_id__channel_name
