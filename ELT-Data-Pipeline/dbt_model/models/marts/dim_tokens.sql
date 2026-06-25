{{ config(
    materialized = 'table',
    alias = 'tokens',
    schema = 'facts'
) 
}}
select token_address,
    token_name,
    symbol,
    decimals,
    total_supply,
    supply_source
from {{ ref('stg_token_metadata') }}