# Shifting Gears: Building an ELT Pipeline for Blockchain Data

Welcome back! In our previous article, we explored the traditional ETL approach by building a pipeline that extracted blockchain data, transformed it externally using PySpark, and finally loaded it into a DuckDB warehouse. While ETL has served as the industry standard for years, modern data engineering is increasingly leaning towards the ELT (Extract, Load, Transform) paradigm. 

Why the shift? Modern data warehouses and databases have become incredibly powerful. Rather than moving massive volumes of data out to a separate processing cluster (like Spark) just to transform it, it is often much faster and more cost-effective to load the raw data directly into the database and transform it in place. In this continuation of our pipeline journey, we will demonstrate how to pivot our architecture to an ELT model using PostgreSQL and dbt (Data Build Tool).

## Extraction and Direct Loading

Our source definition remains identical to our previous build. Blockchain data is vast and requires specialized infrastructure to fetch efficiently, so we continue to rely on SQD's streaming endpoints to retrieve historical blocks, transactions, traces, and logs.

The major paradigm shift happens the moment the data is extracted. Instead of saving intermediate Parquet files for an external system to process, we stream the raw payloads directly into PostgreSQL landing tables. 

Let's look at how our Python extraction logic handles this direct load:

```python
def save_to_postgres(data_list: List[Dict[str, Any]], data_type: str):
    if not data_list:
        print(f"No data to save for {data_type}.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    print(f"Saving {len(data_list)} {data_type} to PostgreSQL...")
    
    if data_type == "blocks":
        query = """
            INSERT INTO blocks (number, timestamp, hash, gas_used, gas_limit)
            VALUES %s ON CONFLICT (number) DO NOTHING
        """
        values = [(d['number'], datetime.fromtimestamp(d['timestamp'], tz=timezone.utc), d['hash'], d['gas_used'], d['gas_limit']) for d in data_list]
    
    elif data_type == "transactions":
        query = """
            INSERT INTO transactions (hash, block_number, transaction_index, from_address, to_address, value, gas, gas_price)
            VALUES %s ON CONFLICT (hash) DO NOTHING
        """
        # (values construction truncated for brevity)
```

In this setup, we use the `psycopg2` library to execute bulk inserts. Notice the use of the `ON CONFLICT DO NOTHING` SQL clause. Just like the idempotent DuckDB upserts in our ETL pipeline, this ensures that if our pipeline fails and restarts, we won't accidentally duplicate data. Additionally, we run a secondary extraction step to fetch missing token metadata via an Ethereum RPC node, pushing that data directly into a `token_metadata` table as well. 

## In-Database Transformation with dbt

With our raw data resting securely in PostgreSQL, we arrive at the Transformation phase. Since this is an ELT pipeline, we don't spin up a Spark cluster. Instead, we bring the compute to the data.

To manage these in-database transformations, we utilize **dbt (Data Build Tool)**. dbt is an industry-standard framework that allows data engineers to write modular, testable SQL queries. It handles the boilerplate of creating views and tables, letting us focus purely on the business logic.

We organize our dbt project into three distinct layers:
1. **Staging**: Casts raw hex strings to numeric types and standardizes column names.
2. **Intermediate**: Applies complex logic, such as separating native Ethereum transfers from ERC-20 token events.
3. **Marts**: Creates the final, aggregated dimensional models (like daily network activity) meant for downstream BI dashboards.

Here is an excerpt from our intermediate layer, designed to isolate native Ethereum transfers:

```sql
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
```

In this snippet, dbt's `{{ ref('stg_traces') }}` macro dynamically references our staging model. When we execute the pipeline, dbt compiles this into raw SQL and pushes it to PostgreSQL. The database engine executes the query natively, vastly reducing data movement and network I/O.

## Orchestration and State Management

Just like our ETL pipeline, scheduling and dependency management are critical to ensuring data integrity. We continue to rely on Apache Airflow for robust orchestration.

Because we are utilizing an ELT architecture, Airflow's primary job is simply to trigger our various systems in the correct order. We define a straightforward TaskFlow DAG: `Extract Data >> Fetch Metadata >> Run dbt Models`. 

Airflow kicks off our Python extraction script, waits for the raw data to safely land in PostgreSQL, and then hands off the transformation workload by invoking `dbt run`. To manage state, we track our high-water mark directly in a PostgreSQL table, ensuring each scheduled Airflow run dynamically picks up exactly where the last one left off without any hardcoded bounds.

## Conclusion

Transitioning from ETL to ELT simplifies our infrastructure by removing the need for an external processing cluster, instead centralizing our data logic directly within the warehouse. By leveraging SQD for rapid extraction, PostgreSQL for storage, and dbt for powerful in-database transformations, we've built a scalable, modern data stack perfectly suited for massive blockchain datasets.

You can check out the repository to dive into the full ELT implementation here [insert repo link here]. Happy data engineering!
