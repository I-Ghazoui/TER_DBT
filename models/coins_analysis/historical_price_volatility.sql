{{ config(
    materialized='incremental',
    unique_key='(id, creation_date)'
) }}

{% if is_incremental() %}
    {% set max_date_query %}
        SELECT COALESCE(MAX(creation_date), '1970-01-01'::TIMESTAMP) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

SELECT
    symbol,
    name,
    AVG((high_24h - low_24h) / NULLIF(low_24h, 0) * 100) AS avg_volatility_percentage
FROM {{ ref('transformed_coingecko_data_v') }}
WHERE 1=1
    AND id IS NOT NULL
    AND id != ' '
    AND name IS NOT NULL
    AND name != ' '
    AND symbol IS NOT NULL
    AND symbol != ' '
    {% if is_incremental() %}
        AND creation_date > '{{ max_date }}' -- Date pré-calculée
    {% endif %}
GROUP BY symbol, name
ORDER BY avg_volatility_percentage DESC