{{
  config(
    materialized='incremental',
    unique_key='link_order_order_line_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  -- Union all sources where link_order_order_line_hk appears
  
  -- Source 1: Order Lines
  SELECT
    link_order_order_line_hk,
    order_hk,
    order_line_hk
  FROM {{ ref('stg_order_lines') }}
  
  UNION ALL
  
  -- Source 2: Order Returns
  SELECT
    link_order_order_line_hk,
    order_hk,
    order_line_hk
  FROM {{ ref('stg_order_returns') }}
),

distinct_links AS (
  SELECT DISTINCT
    link_order_order_line_hk,
    order_hk,
    order_line_hk
  FROM source_data
  
  {% if is_incremental() %}
  WHERE link_order_order_line_hk NOT IN (SELECT link_order_order_line_hk FROM {{ this }})
  {% endif %}
)

SELECT
  link_order_order_line_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  order_hk,
  order_line_hk
FROM distinct_links