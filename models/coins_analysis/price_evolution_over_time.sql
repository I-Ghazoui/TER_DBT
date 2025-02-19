{{ config(materialized='view') }}

WITH price_evolution AS (
    SELECT 
        symbol,
        coin_name,
        current_price,
        creation_date
    FROM {{ ref('transformed_coingecko_data') }}
)

SELECT *
FROM price_evolution
ORDER BY symbol, creation_date;
