with source as (
      select * from {{ source('raw_csv', 'monthly_shorts_non_music_subscription_video_summary') }}
),
renamed as (

    select
        adjustment_type,
        day as date_key,
        country,
        video_id,
        custom_id,
        content_type,
        video_title,
        video_duration__sec,
        username,
        uploader,
        {{ clean_channel_id('channel_id') }} as channel_id,
        owned_subscription_views,
        coalesce(partner_revenue, 0.0) as partner_revenue,
        _file_name as file_name,
        _content_owner as content_owner,
        _report_date_key as report_date_key,
        _file_load_date as file_load_date

    from source
)
select * from renamed
