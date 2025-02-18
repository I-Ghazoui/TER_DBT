WITH coin_price_volatility AS (
    SELECT
        symbol,
        NAME,
        high_24h,
        low_24h,
        (high_24h - low_24h) / low_24h * 100 AS price_volatility_percent,
        creation_date
    FROM {{ ref('transformed_coingecko_data') }}
    WHERE HIGH_24H IS NOT NULL
        AND LOW_24H IS NOT NULL
)

SELECT *
FROM coin_price_volatility
ORDER BY price_volatility_percent DESC
LIMIT 15
