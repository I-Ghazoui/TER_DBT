WITH daily_data AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        AVG(CLOSE_PRICE) AS CLOSE_PRICE
    FROM {{ ref('transformed_coingecko_ohlc_v') }}
    GROUP BY CRYPTO_ID, OHLC_TIMESTAMP::DATE
),

daily_data_with_lag AS (
    SELECT
        CRYPTO_ID,
        TRADE_DATE,
        CLOSE_PRICE,
        LAG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE) AS PREV_CLOSE_PRICE
    FROM daily_data
),

market_cap_daily AS (
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME,
        AVG(MARKET_CAP) AS MARKET_CAP,
        CREATION_DATE::DATE AS TRADE_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
    GROUP BY ID, SYMBOL, NAME, CREATION_DATE::DATE
),

market_cap_with_lag AS (
    SELECT
        CRYPTO_ID,
        SYMBOL,
        NAME,
        TRADE_DATE,
        MARKET_CAP,
        LAG(MARKET_CAP) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE) AS PREV_MARKET_CAP
    FROM market_cap_daily
)

SELECT 
    ddt.CRYPTO_ID,
    mcd.SYMBOL,
    mcd.NAME,
    ddt.TRADE_DATE,
    ((ddt.CLOSE_PRICE - ddt.PREV_CLOSE_PRICE) / NULLIF(ddt.PREV_CLOSE_PRICE, 0)) * 100 AS PRICE_CHANGE_PERCENTAGE,
    ((mcd.MARKET_CAP - mcd.PREV_MARKET_CAP) / NULLIF(mcd.PREV_MARKET_CAP, 0)) * 100 AS MARKET_CAP_CHANGE_PERCENTAGE
FROM daily_data_with_lag ddt
JOIN market_cap_with_lag mcd
    ON ddt.CRYPTO_ID = mcd.CRYPTO_ID AND ddt.TRADE_DATE = mcd.TRADE_DATE
WHERE ddt.PREV_CLOSE_PRICE IS NOT NULL
ORDER BY mcd.NAME ASC, ddt.TRADE_DATE DESC;
