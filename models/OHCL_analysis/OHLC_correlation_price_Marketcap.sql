CREATE OR REPLACE VIEW TER_DATABASE.TER_ANALYSIS_DATA.OHLC_CORRELATION_PRICE_MARKETCAP (
    CRYPTO_ID,
    SYMBOL,
    NAME,
    TRADE_DATE,
    PRICE_CHANGE_PERCENTAGE,
    MARKET_CAP_CHANGE_PERCENTAGE
) AS
WITH transformed_metadata AS (
    -- Extraire les métadonnées des cryptomonnaies depuis la table transformée
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME
    FROM {{ ref('transformed_coingecko_data_v') }}
),
daily_data AS (
    -- Calculer le prix de clôture quotidien en joignant avec les métadonnées
    SELECT 
        t.CRYPTO_ID,
        c.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        MAX(c.CLOSE_PRICE) AS CLOSE_PRICE
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA c
    JOIN transformed_metadata t
        ON c.CRYPTO_ID = t.CRYPTO_ID -- Assurez-vous que cette jointure est correcte
    GROUP BY t.CRYPTO_ID, c.OHLC_TIMESTAMP::DATE
),
price_changes AS (
    -- Calculer les variations de prix quotidiennes
    SELECT 
        d.CRYPTO_ID,
        d.TRADE_DATE,
        d.CLOSE_PRICE,
        LAG(d.CLOSE_PRICE) OVER (PARTITION BY d.CRYPTO_ID ORDER BY d.TRADE_DATE) AS PREV_CLOSE_PRICE
    FROM daily_data d
),
market_cap_daily AS (
    -- Extraire les données de capitalisation boursière quotidienne
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME,
        MARKET_CAP,
        CREATION_DATE::DATE AS TRADE_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
),
market_cap_changes AS (
    -- Calculer les variations de capitalisation boursière quotidiennes
    SELECT 
        m.CRYPTO_ID,
        m.SYMBOL,
        m.NAME,
        m.TRADE_DATE,
        m.MARKET_CAP,
        LAG(m.MARKET_CAP) OVER (PARTITION BY m.CRYPTO_ID ORDER BY m.TRADE_DATE) AS PREV_MARKET_CAP
    FROM market_cap_daily m
)
-- Résultat final : calculer les pourcentages de variation du prix et de la capitalisation boursière
SELECT 
    p.CRYPTO_ID,
    c.SYMBOL,
    c.NAME,
    p.TRADE_DATE,
    (p.CLOSE_PRICE - p.PREV_CLOSE_PRICE) / NULLIF(p.PREV_CLOSE_PRICE, 0) * 100 AS PRICE_CHANGE_PERCENTAGE,
    (c.MARKET_CAP - c.PREV_MARKET_CAP) / NULLIF(c.PREV_MARKET_CAP, 0) * 100 AS MARKET_CAP_CHANGE_PERCENTAGE
FROM price_changes p
JOIN market_cap_changes c
    ON p.CRYPTO_ID = c.CRYPTO_ID AND p.TRADE_DATE = c.TRADE_DATE
WHERE p.PREV_CLOSE_PRICE IS NOT NULL
ORDER BY c.NAME ASC, p.TRADE_DATE DESC