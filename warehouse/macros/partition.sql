{% macro reprocess_dates() -%}
select business_date from meta.reprocess_dates
where run_id = '{{ env_var("COMMERCE_RUN_ID", "manual") | replace("'", "''") }}'
{%- endmacro %}

{% macro delete_reprocess_partition(date_column) -%}
{% if is_incremental() %}
delete from {{ this }} where {{ adapter.quote(date_column) }} in ({{ reprocess_dates() }})
{% else %}
select 1
{% endif %}
{%- endmacro %}

{% macro selected_dates(date_expression) -%}
{% if is_incremental() %}
where {{ date_expression }} in ({{ reprocess_dates() }})
{% endif %}
{%- endmacro %}

{% macro history_key(id_expression, timestamp_expression) -%}
md5({{ id_expression }} || '|' || to_char({{ timestamp_expression }} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'))
{%- endmacro %}
