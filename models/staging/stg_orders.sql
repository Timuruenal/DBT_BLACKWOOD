WITH parsed_data AS (
  -- Parse VARIANT data into typed columns
  SELECT
    RAW_DATA:order_id::VARCHAR            AS order_id,
    RAW_DATA:customer_id::VARCHAR         AS customer_id,
    RAW_DATA:timestamp::BIGINT            AS timestamp,
    RAW_DATA:total_amount::FLOAT          AS total_amount,
    RAW_DATA:item_count::NUMBER           AS item_count,
    RAW_DATA:status::VARCHAR              AS status
    
  FROM {{ source('blackwood', 'orders') }}
)

SELECT
  -- Business Keys
  order_id,
  customer_id,
  
  -- Descriptive Attributes
  {{convert_unix_to_timestamp('timestamp')}} AS timestamp,
  total_amount,
  item_count,
  status,
  
  -- Metadata
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  
  -- Calculated Hash Keys
  MD5(order_id) AS order_hk,
  MD5(customer_id) AS customer_hk,
  
  -- Link Hash Key: alphabetically sorted
  MD5(CONCAT(
    MD5(customer_id), 
    '||', 
    MD5(order_id)
  )) AS link_customer_order_hk,
  
  -- Hash Diff for Satellite
  MD5(CONCAT_WS('||',
    COALESCE(TO_VARCHAR(timestamp), '~'),
    COALESCE(total_amount::VARCHAR, '~'),
    COALESCE(item_count::VARCHAR, '~'),
    COALESCE(status, '~')
  )) AS hash_diff
  
FROM parsed_data