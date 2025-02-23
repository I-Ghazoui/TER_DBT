WITH filtered_cryptos AS (
    -- Filtrer uniquement les cryptomonnaies spécifiées
    SELECT 
        ID,
        SYMBOL
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE SYMBOL IN ('btc', 'eth', 'xrp', 'bnb', 'sol', 'doge', 'ada', 'trx', 'link', 'avax', 'sui', 'xlm', 'litecoin', 'ton', 'shib', 'leo', 'om', 'hype', 'dot', 'uni', 'xmr', 'near', 'pepe', 'apt', 'dai', 'icp')
),
daily_averages AS (
    -- Calculer les moyennes quotidiennes pour chaque cryptomonnaie
    SELECT 
        c.CRYPTO_ID,
        c.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        AVG(c.OPEN_PRICE) AS AVG_OPEN_PRICE,
        AVG(c.HIGH_PRICE) AS AVG_HIGH_PRICE,
        AVG(c.LOW_PRICE) AS AVG_LOW_PRICE,
        AVG(c.CLOSE_PRICE) AS AVG_CLOSE_PRICE
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA c
    JOIN filtered_cryptos f
        ON c.CRYPTO_ID = f.ID
    GROUP BY c.CRYPTO_ID, c.OHLC_TIMESTAMP::DATE
),
price_trends AS (
    -- Calculer les moyennes mobiles sur 7 jours et 30 jours
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        AVG_CLOSE_PRICE,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM daily_averages
),
trend_analysis AS (
    -- Analyser les tendances (Uptrend, Downtrend, Neutral)
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
    JOIN filtered_cryptos c
        ON p.CRYPTO_ID = c.ID
)
-- Résultat final : afficher les données pour les cryptomonnaies spécifiées
SELECT 
    SYMBOL,
    TRADE_DATE,
    AVG_CLOSE_PRICE,
    MA_7D,
    MA_30D,
    TREND_STATUS
FROM trend_analysis
ORDER BY SYMBOL ASC, TRADE_DATE DESC