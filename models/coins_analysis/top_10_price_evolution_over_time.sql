WITH latest_top_10_cryptos AS (
    SELECT 
        SYMBOL,
        NAME
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE CREATION_DATE = (
        SELECT MAX(CREATION_DATE) 
        FROM {{ ref('transformed_coingecko_data_v') }}
    )
    AND SYMBOL NOT IN ('usdt', 'usdc', 'usds') -- We ignore USDT and USDC cuz they are stable coins, we can't really trade them.
    ORDER BY MARKET_CAP_RANK
    LIMIT 10
),

price_evolution_top_10 AS (
    SELECT 
        SYMBOL,
        NAME,
        CURRENT_PRICE,
        ATH,
        CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE (SYMBOL, NAME) IN (SELECT SYMBOL, NAME FROM latest_top_10_cryptos)
),

distance_to_ath AS (
    SELECT 
        SYMBOL,
        NAME,
        CURRENT_PRICE,
        ATH,
        CREATION_DATE,
        ((ATH - CURRENT_PRICE) / NULLIF(ATH,0)) * 100 AS DISTANCE_TO_ATH_PERCENTAGE
    FROM price_evolution_top_10
)

SELECT *
FROM distance_to_ath
ORDER BY SYMBOL, CREATION_DATE