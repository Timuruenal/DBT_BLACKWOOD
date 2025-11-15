{{
  config(
    materialized='incremental',
    unique_key=['order_hk', 'load_date'],
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT
    order_hk,
    load_date,
    timestamp,
    total_amount,
    item_count,
    status,
    hash_diff,
    record_source
  FROM {{ ref('stg_orders') }}
  
  {% if is_incremental() %}
  -- INSERT-only: Füge nur neue Records hinzu (neue hash_diff oder neues load_date)
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.order_hk = {{ ref('stg_orders') }}.order_hk
      AND sat.hash_diff = {{ ref('stg_orders') }}.hash_diff
  )
  {% endif %}
)

SELECT
  order_hk,
  load_date,
  timestamp,
  total_amount,
  item_count,
  status,
  hash_diff,
  record_source
FROM source_data
