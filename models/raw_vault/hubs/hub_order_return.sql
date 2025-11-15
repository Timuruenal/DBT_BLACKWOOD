{{
  config(
    materialized='incremental',
    unique_key='return_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT DISTINCT
    return_hk,
    return_id
  FROM {{ ref('stg_order_returns') }}
  
  {% if is_incremental() %}
  WHERE return_hk NOT IN (SELECT return_hk FROM {{ this }})
  {% endif %}
)

SELECT
  return_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  return_id
FROM source_data