{{
  config(
    materialized='incremental',
    unique_key=['return_hk', 'load_date'],
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT
    return_hk,
    load_date,
    return_timestamp,
    hash_diff,
    record_source
  FROM {{ ref('stg_order_returns') }}
  
  {% if is_incremental() %}
  -- INSERT-only: Füge nur neue Records hinzu (neue hash_diff oder neues load_date)
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.return_hk = {{ ref('stg_order_returns') }}.return_hk
      AND sat.hash_diff = {{ ref('stg_order_returns') }}.hash_diff
  )
  {% endif %}
)

SELECT
  return_hk,
  load_date,
  return_timestamp,
  hash_diff,
  record_source
FROM source_data
