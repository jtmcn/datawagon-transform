with source as (
      select * from {{ source('raw_csv', 'monthly_shorts_non_music_ads_video_summary') }}
),
renamed as (
    select
        adjustment_type,
        video_id,
        video_title,
        video_duration__sec,
        category,
        {{ clean_channel_id('channel_id') }} as channel_id,
        uploader,
        content_type,
        policy,
        total_views,
        coalesce(net_partner_revenue__post_revshare, 0.0) as partner_revenue,
        _file_name as file_name,
        _content_owner as content_owner,
        _report_date_key as report_date_key,
        _file_load_date as file_load_date

    from source
)
select * from renamed
  