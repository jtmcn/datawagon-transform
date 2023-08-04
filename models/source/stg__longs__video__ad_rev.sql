with source as (
      select * from {{ source('raw_csv', 'video_raw') }}
),
renamed as (
    select
        adjustment_type,
        day as date_key,
        country,
        video_id,
        video_title,
        video_duration__sec,
        category,
        {{ clean_channel_id('channel_id') }} as channel_id,
        uploader,
        channel_display_name,
        content_type,
        policy,
        owned_views,
        youtube_revenue_split__auction,
        youtube_revenue_split__reserved,
        youtube_revenue_split__partner_sold_youtube_served,
        youtube_revenue_split__partner_sold_partner_served,
        youtube_revenue_split,
        partner_revenue__auction,
        partner_revenue__reserved,
        partner_revenue__partner_sold_youtube_served,
        partner_revenue__partner_sold_partner_served,
        coalesce(partner_revenue, 0.0) as partner_revenue,
        _file_name as file_name,
        _content_owner as content_owner,
        _report_date_key as report_date_key,
        _file_load_date as file_load_date

    from source
)
select * from renamed
  