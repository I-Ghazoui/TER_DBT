WITH daily_data AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        AVG(CLOSE_PRICE) AS CLOSE_PRICE -- Calcul de la moyenne du prix de clôture quotidien
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
SELECT 
    SYMBOL,
    TRADE_DATE,
    AVG(CLOSE_PRICE) AS AVG_CLOSE_PRICE, -- Calcul de la moyenne du prix de clôture pour chaque coin/jour
    AVG(MA_7D) AS AVG_MA_7D, -- Moyenne des moyennes mobiles sur 7 jours
    AVG(MA_30D) AS AVG_MA_30D, -- Moyenne des moyennes mobiles sur 30 jours
    CASE 
        WHEN AVG(MA_7D) > AVG(MA_30D) THEN 'Uptrend'
        WHEN AVG(MA_7D) < AVG(MA_30D) THEN 'Downtrend'
        ELSE 'Neutral'
    END AS FINAL_TREND_STATUS
FROM trend_analysis
GROUP BY SYMBOL, TRADE_DATE
HAVING COUNT(*) > 0 -- Assurez-vous qu'il y a au moins une ligne pour chaque combinaison SYMBOL/TRADE_DATE
ORDER BY SYMBOL ASC, TRADE_DATE DESC