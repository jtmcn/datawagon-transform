with source as (

    select *
    from {{ source('raw_csv', 'red_rawdata_asset') }}

),

renamed as (

    select
        adjustment_type,
        day as date_key,
        country as country_code,
        asset_id,
        asset_labels,
        {{ clean_channel_id('asset_channel_id') }} as asset_channel_id,
        custom_id,
        asset_title,
        owned_watchtime,
        {{ zeroed('partner_revenue') }},
        _file_name as file_name,
        _content_owner as content_owner,
        _report_date_key as report_date_key,
        _file_load_date as file_load_date
    from source

)

select * from renamed
