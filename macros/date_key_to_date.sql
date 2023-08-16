{% macro date_key_to_date(date_key) %}
    case
        when {{ date_key }} is null
        then null
        else to_date(cast({{ date_key }} as varchar), 'YYYYMMDD')
    end
{% endmacro %} 