{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

with base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    {% if is_incremental() %}
       where creation_date > (select max(creation_date) from {{ this }})
    {% endif %}
)

select
    id,
    name,
    avg((high_24h - low_24h) / nullif(low_24h, 0) * 100) as avg_volatility_percentage
from base
group by id, name
order by avg_volatility_percentage desc
