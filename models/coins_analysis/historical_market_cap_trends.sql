{{
    config(
        materialized='incremental',
        unique_key=["symbol", "date"], 
        merge_update_columns=["market_cap"]
    )
}}

{% if is_incremental() %}
    {% set max_date_query %}
        SELECT COALESCE(MAX(date), '1970-01-01'::DATE) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

WITH deduplicated_data AS (
    SELECT
        CAST(creation_date AS DATE) AS date,
        symbol,
        name,
        market_cap,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, CAST(creation_date AS DATE) 
            ORDER BY creation_date DESC
        ) AS rn
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE symbol IN ('btc', 'eth', 'usdt', 'sol', 'xrp', 'doge', 'trx', 'ada', 'shib')
    {% if is_incremental() %}
        AND creation_date > '{{ max_date }}'
    {% endif %}
)

SELECT
    date,
    symbol,
    name,
    market_cap
FROM deduplicated_data
WHERE rn = 1  -- ◀ Garde seulement la dernière entrée par (symbol, date)
ORDER BY date ASC