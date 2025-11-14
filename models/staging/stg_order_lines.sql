WITH parsed_data AS (
  -- Parse VARIANT data into typed columns
  SELECT
    RAW_DATA:order_line_id::VARCHAR       AS order_line_id,
    RAW_DATA:order_id::VARCHAR            AS order_id,
    RAW_DATA:product_id::VARCHAR          AS product_id,
    RAW_DATA:timestamp::BIGINT            AS timestamp,
    RAW_DATA:product_name::VARCHAR        AS product_name,
    RAW_DATA:category::VARCHAR            AS category,
    RAW_DATA:collection::VARCHAR          AS collection,
    RAW_DATA:quantity::NUMBER             AS quantity,
    RAW_DATA:unit_price::FLOAT            AS unit_price,
    RAW_DATA:line_total::FLOAT            AS line_total
    
  FROM {{ source('blackwood', 'order_lines') }}
)

SELECT
  -- Business Keys
  order_line_id,
  order_id,
  product_id,
  
  -- Descriptive Attributes
  {{convert_unix_to_timestamp('timestamp')}} AS timestamp,
  product_name,
  category,
  collection,
  quantity,
  unit_price,
  line_total,
  
  -- Metadata
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  
  -- Calculated Hash Keys
  MD5(order_line_id) AS order_line_hk,
  MD5(order_id) AS order_hk,
  MD5(product_id) AS product_hk,
  
  -- Link Hash Keys (alphabetically sorted)
  MD5(CONCAT(
    MD5(order_line_id),
    '||',
    MD5(product_id)
  )) AS link_order_line_product_hk,
  
  MD5(CONCAT(
    MD5(order_id),
    '||',
    MD5(order_line_id)
  )) AS link_order_order_line_hk,
  
  -- Hash Diff for Satellite
  MD5(CONCAT_WS('||',
    COALESCE(product_name, '~'),
    COALESCE(category, '~'),
    COALESCE(collection, '~'),
    COALESCE(quantity::VARCHAR, '~'),
    COALESCE(unit_price::VARCHAR, '~'),
    COALESCE(line_total::VARCHAR, '~')
  )) AS hash_diff
  
FROM parsed_data