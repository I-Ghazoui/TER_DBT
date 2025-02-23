WITH price_changes AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        CLOSE_PRICE,
        LAG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY OHLC_TIMESTAMP) AS PREV_CLOSE_PRICE
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA
),
market_cap_changes AS (
    SELECT 
        ID AS CRYPTO_ID,
        MARKET_CAP,
        CREATION_DATE::DATE AS TRADE_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
)
SELECT 
    p.CRYPTO_ID,
    c.SYMBOL,
    c.NAME,
    p.TRADE_DATE,
    (p.CLOSE_PRICE - p.PREV_CLOSE_PRICE) / NULLIF(p.PREV_CLOSE_PRICE, 0) * 100 AS PRICE_CHANGE_PERCENTAGE,
    (c.MARKET_CAP - LAG(c.MARKET_CAP) OVER (PARTITION BY c.CRYPTO_ID ORDER BY c.TRADE_DATE)) / NULLIF(LAG(c.MARKET_CAP), 0) * 100 AS MARKET_CAP_CHANGE_PERCENTAGE
FROM price_changes p
JOIN market_cap_changes c
    ON p.CRYPTO_ID = c.CRYPTO_ID AND p.TRADE_DATE = c.TRADE_DATE
WHERE p.PREV_CLOSE_PRICE IS NOT NULL
ORDER BY p.TRADE_DATE DESC