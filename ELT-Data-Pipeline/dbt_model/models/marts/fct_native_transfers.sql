{{ config(
    materialized = 'table',
    alias = 'native_transfers',
    schema = 'facts'
) }} with native_transfers as (
    select *
    from {{ ref('int_native_transfers') }}
),
blocks as (
    select *
    from {{ ref('stg_blocks') }}
)
select nt.trace_id,
    nt.transaction_hash,
    nt.block_number,
    b.block_timestamp,
    nt.from_address,
    nt.to_address,
    nt.eth_value
from native_transfers nt
    left join blocks b on nt.block_number = b.block_number