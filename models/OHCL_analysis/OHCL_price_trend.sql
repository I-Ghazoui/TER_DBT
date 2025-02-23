WITH daily_averages AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE, -- Tronquer le timestamp à la date
        AVG(OPEN_PRICE) AS AVG_OPEN_PRICE,
        AVG(HIGH_PRICE) AS AVG_HIGH_PRICE,
        AVG(LOW_PRICE) AS AVG_LOW_PRICE,
        AVG(CLOSE_PRICE) AS AVG_CLOSE_PRICE
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA
    GROUP BY CRYPTO_ID, OHLC_TIMESTAMP::DATE
),
price_trends AS (
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        AVG_CLOSE_PRICE,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM daily_averages
),
trend_analysis AS (
    SELECT 
        p.CRYPTO_ID,
        c.SYMBOL,
        p.TRADE_DATE,
        p.AVG_CLOSE_PRICE,
        p.MA_7D,
        p.MA_30D,
        CASE 
            WHEN p.MA_7D > p.MA_30D THEN 'Uptrend'
            WHEN p.MA_7D < p.MA_30D THEN 'Downtrend'
            ELSE 'Neutral'
        END AS TREND_STATUS
    FROM price_trends p
    JOIN {{ ref('transformed_coingecko_data_v') }} c
        ON p.CRYPTO_ID = c.ID
)
SELECT 
    SYMBOL,
    TRADE_DATE,
    AVG_CLOSE_PRICE,
    MA_7D,
    MA_30D,
    TREND_STATUS
FROM trend_analysis
ORDER BY SYMBOL ASC, TRADE_DATE DESC