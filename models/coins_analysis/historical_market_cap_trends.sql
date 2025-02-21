{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

with base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    {% if is_incremental() %}
       where creation_date > (select max(creation_date) from {{ this }})
    {% endif %}
)

select
    cast(creation_date as date) as date,
    id,
    name,
    market_cap
from base
order by date asc
