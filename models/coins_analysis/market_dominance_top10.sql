WITH top_10_market_dominance AS (
    SELECT 
        SYMBOL,
        NAME,
        MARKET_CAP
    FROM {{ ref('latest_transformed_coingecko_data_v') }}
    WHERE MARKET_CAP_RANK <= 10
)

SELECT *
FROM top_10_market_dominance
ORDER BY MARKET_CAP DESC;
