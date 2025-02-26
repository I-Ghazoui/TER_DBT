{{ config(
    materialized='table'
) }}

WITH collection_sales AS (
    SELECT 
        collection_name,
        SUM(price) AS total_sales,
        COUNT(*) AS total_sales_count
    FROM {{ ref('stg_nft_events') }}
    WHERE collection_name IS NOT NULL
    GROUP BY collection_name
)

SELECT 
    collection_name,
    total_sales,
    total_sales_count
FROM collection_sales
ORDER BY total_sales DESC
LIMIT 10