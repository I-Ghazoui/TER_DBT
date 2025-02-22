{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

WITH base AS (
    SELECT 
        SYMBOL,
        NAME,
        CURRENT_PRICE,
        HIGH_24H,
        LOW_24H,
        ATH,
        ATL,
        CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE 1=1
    {% if is_incremental() %}
       AND CREATION_DATE > (SELECT MAX(CREATION_DATE) FROM {{ this }})
    {% endif %}
    AND ID IS NOT NULL
    AND ID != ' '
    AND NAME IS NOT NULL
    AND NAME != ' '
    AND SYMBOL IS NOT NULL
    AND SYMBOL != ' '
    AND SYMBOL NOT IN ('usdt', 'usdc', 'usds', 'wbtc', 'steth') -- Exclustion de  USDT et USDC
),

volatility_analysis AS (
    SELECT 
        SYMBOL,
        NAME,
        CREATION_DATE,
        (HIGH_24H - LOW_24H) / NULLIF(CURRENT_PRICE, 0) * 100 AS RELATIVE_VOLATILITY_PERCENTAGE,
        (CURRENT_PRICE - ATH) / NULLIF(ATH, 0) * 100 AS DISTANCE_TO_ATH_PERCENTAGE,
        (CURRENT_PRICE - ATL) / NULLIF(ATL, 0) * 100 AS DISTANCE_TO_ATL_PERCENTAGE
    FROM base
)

SELECT 
    SYMBOL,
    NAME,
    AVG(RELATIVE_VOLATILITY_PERCENTAGE) AS AVG_RELATIVE_VOLATILITY,
    AVG(DISTANCE_TO_ATH_PERCENTAGE) AS AVG_DISTANCE_TO_ATH,
    AVG(DISTANCE_TO_ATL_PERCENTAGE) AS AVG_DISTANCE_TO_ATL
FROM volatility_analysis

GROUP BY SYMBOL, NAME
ORDER BY AVG_RELATIVE_VOLATILITY DESC

-- We can use Scatter plot for this one.