{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

{% if is_incremental() %}
with max_date as (
    select coalesce(max(creation_date), '1970-01-01'::timestamp) as max_creation_date
    from {{ this }}
),
{% endif %}

base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    where 1=1
    {% if is_incremental() %}
        and creation_date > (select max_creation_date from max_date)
    {% endif %}
    and id is not null
    and id != ' '
    and name is not null
    and name != ' '
    and symbol is not null
    and symbol != ' '
)

select
    symbol,
    name,
    avg((high_24h - low_24h) / nullif(low_24h, 0) * 100) as avg_volatility_percentage
from base
group by symbol, name
order by avg_volatility_percentage desc