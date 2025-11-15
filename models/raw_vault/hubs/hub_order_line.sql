{{
  config(
    materialized='incremental',
    unique_key='order_line_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  -- Union all sources where order_line_id appears
  
  -- Source 1: Order Lines
  SELECT
    order_line_hk,
    order_line_id
  FROM {{ ref('stg_order_lines') }}
  
  UNION ALL
  
  -- Source 2: Order Returns
  SELECT
    order_line_hk,
    order_line_id
  FROM {{ ref('stg_order_returns') }}
),

distinct_order_lines AS (
  SELECT DISTINCT
    order_line_hk,
    order_line_id
  FROM source_data
  
  {% if is_incremental() %}
  WHERE order_line_hk NOT IN (SELECT order_line_hk FROM {{ this }})
  {% endif %}
)

SELECT
  order_line_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  order_line_id
FROM distinct_order_lines