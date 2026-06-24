"""
Stage 2: Enrich
This script takes the cleaned token transfers and joins them with the on-chain 
token metadata (like name, symbol, and decimals). It then uses the decimals 
to convert the raw blockchain values into human-readable token amounts.
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def get_spark():
    return SparkSession.builder.appName("stage2_enrich").master("local[*]").getOrCreate()

TOKEN_METADATA_PATH = "data/reference_data/token_metadata.csv"

def run_stage2(start_block: int, end_block: int):
    spark = get_spark()
    
    base_dir = "data/stage1_data"
    token_transfers_path = f"{base_dir}/token_transfers/block_start={start_block}/block_end={end_block}/data.parquet"
    transactions_path = f"{base_dir}/transactions/block_start={start_block}/block_end={end_block}/data.parquet"
    blocks_path = f"{base_dir}/blocks/block_start={start_block}/block_end={end_block}/data.parquet"
    traces_path = f"{base_dir}/traces/block_start={start_block}/block_end={end_block}/data.parquet"
    
    try:
        token_transfers = spark.read.parquet(token_transfers_path)
        transactions = spark.read.parquet(transactions_path)
        blocks = spark.read.parquet(blocks_path)
        traces = spark.read.parquet(traces_path)
    except Exception as e:
        print(f"Error reading stage1 data: {e}")
        return

    token_metadata = spark.read.option("header", True).csv(TOKEN_METADATA_PATH)
    token_metadata = token_metadata.withColumn("decimals", F.col("decimals").cast("int"))
    
    # Enrichment of token_transfers with the token_metadata u
    enriched_transfers = (
        token_transfers
        .join(F.broadcast(token_metadata), on="token_address", how="left")
        .withColumn(
            "transfer_value_normalized",
            F.col("value") / F.pow(F.lit(10), F.col("decimals"))
        )
    )
    
    enriched_transactions = transactions
    
    out_dir = "data/stage2_data"
    
    def save_df(df, name):
        p = f"{out_dir}/{name}/block_start={start_block}/block_end={end_block}/data.parquet"
        os.makedirs(os.path.dirname(p), exist_ok=True)
        df.write.mode("overwrite").parquet(p)
        print(f"Stage 2 complete for {name}: {df.count()} rows saved to {p}")
        
    save_df(enriched_transfers, "token_transfers")
    save_df(enriched_transactions, "transactions")
    save_df(blocks, "blocks")
    save_df(traces, "traces")
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    args = parser.parse_args()
    run_stage2(args.start_block, args.end_block)
