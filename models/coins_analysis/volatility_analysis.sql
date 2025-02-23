{{ config(
    materialized='incremental',
    unique_key='(symbol, trade_date)'
) }}

{% if is_incremental() %}
    {% set max_date_query %}
        SELECT COALESCE(MAX(creation_date), '1970-01-01'::DATE) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

WITH base AS (
    SELECT 
        id,
        symbol,
        name,
        current_price,
        high_24h,
        low_24h,
        ath,
        atl,
        creation_date::DATE AS trade_date
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE 1=1
        AND id IS NOT NULL
        AND id != ' '
        AND name IS NOT NULL
        AND name != ' '
        AND symbol IS NOT NULL
        AND symbol != ' '
        AND symbol NOT IN ('usdt', 'usdc', 'usds', 'wbtc', 'steth')
        {% if is_incremental() %}
            AND creation_date > '{{ max_date }}' -- Date pré-calculée
        {% endif %}
),

volatility_analysis AS (
    SELECT 
        ID,
        SYMBOL,
        NAME,
        TRADE_DATE,
        (HIGH_24H - LOW_24H) / NULLIF(CURRENT_PRICE, 0) * 100 AS RELATIVE_VOLATILITY_PERCENTAGE,
        (CURRENT_PRICE - ATH) / NULLIF(ATH, 0) * 100 AS DISTANCE_TO_ATH_PERCENTAGE,
        (CURRENT_PRICE - ATL) / NULLIF(ATL, 0) * 100 AS DISTANCE_TO_ATL_PERCENTAGE
    FROM base
    WHERE ATL > 0.0001 -- Exclure les cryptomonnaies avec un ATL extrêmement bas
),
aggregated_metrics AS (
    SELECT 
        SYMBOL,
        NAME,
        AVG(RELATIVE_VOLATILITY_PERCENTAGE) AS AVG_RELATIVE_VOLATILITY,
        AVG(DISTANCE_TO_ATH_PERCENTAGE) AS AVG_DISTANCE_TO_ATH,
        AVG(DISTANCE_TO_ATL_PERCENTAGE) AS AVG_DISTANCE_TO_ATL
    FROM volatility_analysis
    GROUP BY SYMBOL, NAME
)
SELECT 
    SYMBOL,
    NAME,
    AVG_RELATIVE_VOLATILITY,
    AVG_DISTANCE_TO_ATH,
    AVG_DISTANCE_TO_ATL
FROM aggregated_metrics
WHERE AVG_DISTANCE_TO_ATL < 15000 -- Filtrer les cryptomonnaies avec une distance à l'ATL < 15000 %
ORDER BY AVG_RELATIVE_VOLATILITY DESC
LIMIT 20