"""
Stage 4: Materialize
This script builds the final analytical tables (like transactions and transfers), 
runs data quality checks to ensure there is no bad data, and safely appends 
the clean records into the DuckDB data warehouse for analysts to query.
"""

import os
import argparse
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DUCKDB_PATH = "data/warehouse/blockchain.duckdb"

def get_spark():
    return SparkSession.builder.appName("stage4_materialize").master("local[*]").getOrCreate()

def validate(df, name: str, checks: list):
    for check_name, condition in checks:
        failing = df.filter(condition).count()
        if failing > 0:
            raise ValueError(f"Validation failed [{name}] — {check_name}: {failing} failing rows")
    print(f"All validations passed for {name}")

def build_dim_transactions(transactions_df, blocks_df):
    return (
        transactions_df
        .join(blocks_df.select("number", "timestamp", "gas_used"), transactions_df.block_number == blocks_df.number, how="left")
        .withColumn("block_timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("gas_efficiency", F.col("gas_used") / F.col("gas"))
        .drop("number", "timestamp")
    )

def build_fact_transfers(unified_transfers_df, blocks_df):
    return (
        unified_transfers_df
        .join(blocks_df.select("number", "timestamp"), unified_transfers_df.block_number == blocks_df.number, how="left")
        .withColumn("block_timestamp", F.to_timestamp(F.col("timestamp")))
        .drop("number", "timestamp")
    )



def load_to_duckdb(df, table_name: str, start_block: int, end_block: int):
    """
    If the Table doesnt exits, create the tabele,
    If the Table exists, delete the data for the current batch range and insert new data.
    """

    local_path = f"data/stage4_data/{table_name}/block_start={start_block}/block_end={end_block}/data.parquet"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    df.write.mode("overwrite").parquet(local_path)
    
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    
    table_exists = False
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        table_exists = True
    except duckdb.CatalogException:
        pass

    if not table_exists:
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{local_path}')
        """)
    else:
        conn.execute(f"DELETE FROM {table_name} WHERE block_number >= {start_block} AND block_number <= {end_block}")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{local_path}')")
        
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()
    print(f"DuckDB table {table_name} now contains {count} total rows.")

def run_stage4(start_block: int, end_block: int):
    spark = get_spark()
    
    try:
        transactions = spark.read.parquet(f"data/stage2_data/transactions/block_start={start_block}/block_end={end_block}/data.parquet")
        blocks = spark.read.parquet(f"data/stage2_data/blocks/block_start={start_block}/block_end={end_block}/data.parquet")
        unified_transfers = spark.read.parquet(f"data/stage3_data/unified_transfers/block_start={start_block}/block_end={end_block}/data.parquet")
    except Exception as e:
        print(f"Error reading input data: {e}")
        return

    dim_transactions = build_dim_transactions(transactions, blocks)
    fact_transfers = build_fact_transfers(unified_transfers, blocks)

    validate(fact_transfers, "fact_transfers", [
        ("no negative transfer values", F.col("transfer_value_normalized") < 0),
        ("no null transaction hashes", F.col("transaction_hash").isNull()),
    ])
    
    load_to_duckdb(dim_transactions, "dim_transactions", start_block, end_block)
    load_to_duckdb(fact_transfers, "fact_transfers", start_block, end_block)
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    args = parser.parse_args()
    run_stage4(args.start_block, args.end_block)
