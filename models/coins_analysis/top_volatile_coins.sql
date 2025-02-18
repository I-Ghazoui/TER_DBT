WITH coin_price_volatility AS (
    SELECT
        symbol,
        NAME,
        high_24h,
        low_24h,
        (high_24h - low_24h) / low_24h * 100 AS price_volatility_percent,
        creation_date
    FROM {{ ref('transformed_coingecko_data') }}
)

SELECT *
FROM coin_price_volatility
ORDER BY price_volatility_percent DESC
LIMIT 15
