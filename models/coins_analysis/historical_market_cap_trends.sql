{{ config(
    materialized='incremental',
    unique_key='(symbol, date)' --  Correction clé unique
) }}

{% if is_incremental() %}
    {% set max_date_query %}
        SELECT COALESCE(MAX(date), '1970-01-01'::DATE) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

SELECT
    CAST(creation_date AS DATE) AS date, -- Colonne finale : "date"
    symbol,
    name,
    market_cap
FROM {{ ref('transformed_coingecko_data_v') }}
WHERE 1=1
    AND symbol IN ('btc', 'eth', 'usdt', 'sol', 'xrp', 'doge', 'trx', 'ada', 'shib')
    {% if is_incremental() %}
        AND creation_date > '{{ max_date }}' 
    {% endif %}
ORDER BY date ASC