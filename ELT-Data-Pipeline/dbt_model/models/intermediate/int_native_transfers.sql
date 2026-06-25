with traces as (
    select * from {{ ref('stg_traces') }}
)

select
    trace_id,
    transaction_hash,
    block_number,
    from_address,
    to_address,
    eth_value
from traces
where eth_value > 0
