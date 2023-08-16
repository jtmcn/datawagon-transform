with asset_channels as (

    select distinct asset_channel_id
    from
        {{ ref('stg__longs__claim__ad_rev') }}
    where
        asset_channel_id is not null

),

channel_id_and_name as (

    select
        channel_id,
        channel_display_name
    from {{ ref('int__longs__video__ad_rev__daily_channel_rev') }}
    group by
        channel_id,
        channel_display_name

),

int__asset_channels as (

    select
        channel_id as asset_channel_id,
        channel_display_name as asset_channel_display_name
    from channel_id_and_name
    where channel_id in (select asset_channel_id from asset_channels)

)

select *
from int__asset_channels
