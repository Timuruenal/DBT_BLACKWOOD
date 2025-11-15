{{
  config(
    materialized='incremental',
    unique_key=['product_hk', 'load_date'],
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT
    product_hk,
    load_date,
    timestamp,
    current_price,
    sku,
    name,
    category,
    collection,
    base_price,
    launch_date,
    discontinuation_date,
    subcategory,
    description,
    material,
    hash_diff,
    record_source
  FROM {{ ref('stg_products') }}
  
  {% if is_incremental() %}
  -- INSERT-only: Füge nur neue Records hinzu (neue hash_diff oder neues load_date)
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.product_hk = {{ ref('stg_products') }}.product_hk
      AND sat.hash_diff = {{ ref('stg_products') }}.hash_diff
  )
  {% endif %}
)

SELECT
  product_hk,
  load_date,
  timestamp,
  current_price,
  sku,
  name,
  category,
  collection,
  base_price,
  launch_date,
  discontinuation_date,
  subcategory,
  description,
  material,
  hash_diff,
  record_source
FROM source_data
