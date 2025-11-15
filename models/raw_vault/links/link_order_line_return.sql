{{
  config(
    materialized='incremental',
    unique_key='link_order_line_return_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT DISTINCT
    link_order_line_return_hk,
    order_line_hk,
    return_hk
  FROM {{ ref('stg_order_returns') }}
  
  {% if is_incremental() %}
  WHERE link_order_line_return_hk NOT IN (SELECT link_order_line_return_hk FROM {{ this }})
  {% endif %}
)

SELECT
  link_order_line_return_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  order_line_hk,
  return_hk,
FROM source_data