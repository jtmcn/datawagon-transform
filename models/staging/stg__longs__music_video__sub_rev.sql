with source as (
    select * from {{ source('raw_csv', 'red_music_rawdata_video') }}
),

renamed as (
    select
        adjustment_type,
        day as date_key,
        country as country_code,
        video_id,
        custom_id,
        content_type,
        video_title,
        video_duration__sec,
        username,
        uploader,
        channel_display_name,
        {{ clean_channel_id('channel_id') }} as channel_id,
        claim_type,
        claim_origin,
        multiple_claims,
        asset_id,
        policy,
        owned_views,
        {{ zeroed('youtube_revenue_split') }},
        {{ zeroed('partner_revenue') }},
        _file_name as file_name,
        _content_owner as content_owner,
        _report_date_key as report_date_key,
        _file_load_date as file_load_date

    from source
)

select * from renamed
