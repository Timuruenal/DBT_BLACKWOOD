{{
  config(
    materialized='incremental',
    unique_key=['order_line_hk', 'load_date'],
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT
    order_line_hk,
    load_date,
    timestamp,
    product_name,
    category,
    collection,
    quantity,
    unit_price,
    line_total,
    hash_diff,
    record_source
  FROM {{ ref('stg_order_lines') }}
  
  {% if is_incremental() %}
  -- INSERT-only: Füge nur neue Records hinzu (neue hash_diff oder neues load_date)
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.order_line_hk = {{ ref('stg_order_lines') }}.order_line_hk
      AND sat.hash_diff = {{ ref('stg_order_lines') }}.hash_diff
  )
  {% endif %}
)

SELECT
  order_line_hk,
  load_date,
  timestamp,
  product_name,
  category,
  collection,
  quantity,
  unit_price,
  line_total,
  hash_diff,
  record_source
FROM source_data
