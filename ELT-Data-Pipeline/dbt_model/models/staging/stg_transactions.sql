{{ config(
    materialized = 'view',
    alias = 'transactions',
    schema = 'staging'
) }} with raw_transactions as (
    select *
    from {{ source('ethereum', 'transactions') }}
)
select hash as transaction_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value as eth_value,
    gas as gas_provided,
    gas_price
from raw_transactions