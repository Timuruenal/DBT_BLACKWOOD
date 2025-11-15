{{
  config(
    materialized='incremental',
    unique_key='product_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  -- Union all sources where product_id appears
  
  -- Source 1: Products
  SELECT
    product_hk,
    product_id
  FROM {{ ref('stg_products') }}
  
  UNION ALL
  
  -- Source 2: Order Lines
  SELECT
    product_hk,
    product_id
  FROM {{ ref('stg_order_lines') }}
),

distinct_products AS (
  SELECT DISTINCT
    product_hk,
    product_id
  FROM source_data
  
  {% if is_incremental() %}
  WHERE product_hk NOT IN (SELECT product_hk FROM {{ this }})
  {% endif %}
)

SELECT
  product_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  product_id
FROM distinct_products