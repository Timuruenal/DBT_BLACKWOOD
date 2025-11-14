WITH parsed_data AS (
  -- Parse VARIANT data into typed columns
  SELECT
    RAW_DATA:payment_id::VARCHAR          AS payment_id,
    RAW_DATA:order_id::VARCHAR            AS order_id,
    RAW_DATA:timestamp::BIGINT            AS timestamp,
    RAW_DATA:payment_method::VARCHAR      AS payment_method,
    RAW_DATA:payment_status::VARCHAR      AS payment_status,
    RAW_DATA:amount::FLOAT                AS amount,
    RAW_DATA:transaction_reference::VARCHAR AS transaction_reference,
    RAW_DATA:retry_count::NUMBER          AS retry_count,
    RAW_DATA:failure_reason::VARCHAR      AS failure_reason
    
  FROM {{ source('blackwood', 'payments') }}
)

SELECT
  -- Business Keys
  payment_id,
  order_id,
  
  -- Descriptive Attributes
  {{convert_unix_to_timestamp('timestamp')}} AS timestamp,
  payment_method,
  payment_status,
  amount,
  transaction_reference,
  retry_count,
  failure_reason,
  
  -- Metadata
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  
  -- Calculated Hash Keys
  MD5(payment_id) AS payment_hk,
  MD5(order_id) AS order_hk,
  
  -- Link Hash Key (alphabetically sorted)
  MD5(CONCAT(
    MD5(order_id),
    '||',
    MD5(payment_id)
  )) AS link_payment_order_hk,
  
  -- Hash Diff for Satellite
  MD5(CONCAT_WS('||',
    COALESCE(payment_method, '~'),
    COALESCE(payment_status, '~'),
    COALESCE(amount::VARCHAR, '~'),
    COALESCE(transaction_reference, '~'),
    COALESCE(retry_count::VARCHAR, '~'),
    COALESCE(failure_reason, '~')
  )) AS hash_diff
  
FROM parsed_data