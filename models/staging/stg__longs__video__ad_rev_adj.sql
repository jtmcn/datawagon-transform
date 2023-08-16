with source as (
    select * from {{ source('raw_csv', 'adj_video_raw') }}
),

renamed as (
    select
        adjustment_type,
        day as date_key,
        country as country_code,
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
        {{ zeroed('youtube_revenue_split__auction') }},
        {{ zeroed('youtube_revenue_split__reserved') }},
        {{ zeroed('youtube_revenue_split__partner_sold_youtube_served') }},
        {{ zeroed('youtube_revenue_split__partner_sold_partner_served') }},
        {{ zeroed('youtube_revenue_split') }},
        {{ zeroed('partner_revenue__auction') }},
        {{ zeroed('partner_revenue__reserved') }},
        {{ zeroed('partner_revenue__partner_sold_youtube_served') }},
        {{ zeroed('partner_revenue__partner_sold_partner_served') }},
        {{ zeroed('partner_revenue') }},
        _file_name as file_name,
        _content_owner as content_owner,
        _report_date_key as report_date_key,
        _file_load_date as file_load_date

    from source
)

select * from renamed
