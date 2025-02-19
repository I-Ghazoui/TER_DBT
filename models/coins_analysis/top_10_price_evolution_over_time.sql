WITH latest_top_10_cryptos AS (
    SELECT 
        SYMBOL,
        NAME
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE CREATION_DATE = (
        SELECT MAX(CREATION_DATE) 
        FROM {{ ref('transformed_coingecko_data_v') }}
    )
    ORDER BY MARKET_CAP_RANK
    LIMIT 10
),

price_evolution_top_10 AS (
    SELECT 
        SYMBOL,
        NAME,
        CURRENT_PRICE,
        CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE (SYMBOL, NAME) IN (SELECT SYMBOL, NAME FROM latest_top_10_cryptos)
)

SELECT *
FROM price_evolution_top_10
ORDER BY SYMBOL, CREATION_DATE