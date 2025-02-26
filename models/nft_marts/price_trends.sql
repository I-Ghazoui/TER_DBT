{{ config(
    materialized='table'
) }}

SELECT 
    DATE_TRUNC('WEEK', event_timestamp) AS week,
    collection_name,
    AVG(price) AS avg_price
FROM {{ ref('stg_nft_events') }}
WHERE 
    event_type = 'Sale'
    AND collection_name IN (
        SELECT collection_name 
        FROM {{ ref('top_collections') }} 
        LIMIT 5
    )
GROUP BY week, collection_name
ORDER BY week ASC, avg_price DESC
