WITH parsed_data AS (
  -- Parse VARIANT data into typed columns
  SELECT
    RAW_DATA:return_id::VARCHAR                     AS return_id,
    RAW_DATA:original_order_line_id::VARCHAR        AS order_line_id,
    RAW_DATA:order_id::VARCHAR                      AS order_id,
    RAW_DATA:return_timestamp::BIGINT               AS return_timestamp
    
  FROM {{ source('blackwood', 'order_returns') }}
)

SELECT
  -- Business Keys
  return_id,
  order_line_id,
  order_id,
  
  -- Descriptive Attributes
  {{convert_unix_to_timestamp('return_timestamp')}} AS return_timestamp,
  
  -- Metadata
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  
  -- Calculated Hash Keys
  MD5(return_id) AS return_hk,
  MD5(order_line_id) AS order_line_hk,
  MD5(order_id) AS order_hk,
  
  -- Link Hash Keys (alphabetically sorted)
  MD5(CONCAT(
    MD5(order_line_id),
    '||',
    MD5(return_id)
  )) AS link_order_line_return_hk,
  
  MD5(CONCAT(
    MD5(order_id),
    '||',
    MD5(order_line_id)
  )) AS link_order_order_line_hk,
  
  -- Hash Diff for Satellite
  MD5(COALESCE(return_timestamp::VARCHAR, '~')) AS hash_diff
  
FROM parsed_data