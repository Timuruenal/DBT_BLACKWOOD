SELECT *
FROM {{ source('blackwood', 'orders') }}
LIMIT 10