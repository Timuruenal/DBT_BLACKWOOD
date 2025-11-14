{% macro convert_unix_to_timestamp(x) %}

TO_TIMESTAMP_NTZ({{x}} / 1000000)

{% endmacro %}