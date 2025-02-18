-- coins_analysis/top_10_market_cap.sql

WITH ranked_coins AS (
    SELECT 
        SYMBOL,
        NAME,
        CREATION_DATE,
        MARKET_CAP,
        MARKET_CAP_RANK,
        LAG(MARKET_CAP) OVER (PARTITION BY SYMBOL ORDER BY CREATION_DATE) AS PREVIOUS_MARKET_CAP,
        (MARKET_CAP - LAG(MARKET_CAP) OVER (PARTITION BY SYMBOL ORDER BY CREATION_DATE)) / 
            LAG(MARKET_CAP) OVER (PARTITION BY SYMBOL ORDER BY CREATION_DATE) * 100 AS MARKET_CAP_CHANGE_PERCENT
    FROM {{ ref('coingecko_coins_data_transformed_view') }}
    WHERE MARKET_CAP_RANK <= 10
)

SELECT *
FROM ranked_coins
ORDER BY CREATION_DATE DESC
