WITH daily_data AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        MAX(CLOSE_PRICE) AS CLOSE_PRICE -- Prix de clôture quotidien
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA
    GROUP BY CRYPTO_ID, OHLC_TIMESTAMP::DATE
),
price_trends AS (
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        CLOSE_PRICE,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM daily_data
),
trend_analysis AS (
    SELECT 
        p.CRYPTO_ID,
        c.SYMBOL,
        p.TRADE_DATE,
        p.CLOSE_PRICE,
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
SELECT *
FROM trend_analysis
WHERE TREND_STATUS = 'Uptrend' -- Focus on uptrends
ORDER BY SYMBOL ASC, TRADE_DATE DESC