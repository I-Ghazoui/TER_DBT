WITH price_evolution AS (
    SELECT 
        SYMBOL,
        NAME,
        CURRENT_PRICE,
        CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
)

SELECT *
FROM price_evolution
ORDER BY symbol, creation_date