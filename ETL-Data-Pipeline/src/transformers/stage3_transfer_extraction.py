"""
Stage 3: Extract and Unify Transfers
This script extracts internal ETH transfers from the raw blockchain traces.
It then unifies these ETH transfers with the ERC-20 token transfers into 
a single, massive "unified transfers" table so they can be analyzed together.
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

def get_spark():
    return SparkSession.builder.appName("stage3_transfer_extraction").master("local[*]").getOrCreate()

def extract_internal_eth_transfers(traces_df):
    return (
        traces_df
        .filter(F.col("call_type") == "call")
        .filter(F.col("value") > 0)
        .filter(F.col("error").isNull() | (F.col("error") == ""))
        .select(
            F.col("transaction_hash"),
            F.col("block_number"),
            F.col("from_address").cast(StringType()).alias("from_address"),
            F.col("to_address").cast(StringType()).alias("to_address"),
            F.col("value").cast(DecimalType(38, 0)).alias("value"),
            F.lit("ETH").alias("token_address"),
            F.lit("ETH").alias("symbol"),
            F.lit(18).alias("decimals"),
            F.lit("internal").alias("transfer_type"),
        )
        .withColumn(
            "transfer_value_normalized",
            (F.col("value") / F.lit(10 ** 18)).cast("double")
        )
    )

def extract_erc20_transfers(token_transfers_df):
    cols = [
        "transaction_hash", "block_number", "from_address", "to_address",
        "value", "token_address", "symbol", "decimals",
        "transfer_value_normalized"
    ]
    
    existing_cols = token_transfers_df.columns
    select_cols = []
    for c in cols:
        if c in existing_cols:
            select_cols.append(F.col(c))
            
    select_cols.append(F.lit("erc20").alias("transfer_type"))
    
    return token_transfers_df.select(*select_cols)

def run_stage3(start_block: int, end_block: int):
    spark = get_spark()
    
    base_dir = "data/stage2_data"
    try:
        traces = spark.read.parquet(f"{base_dir}/traces/block_start={start_block}/block_end={end_block}/data.parquet")
        token_transfers = spark.read.parquet(f"{base_dir}/token_transfers/block_start={start_block}/block_end={end_block}/data.parquet")
    except Exception as e:
        print(f"Error reading stage2 data: {e}")
        return

    internal_eth = extract_internal_eth_transfers(traces)
    erc20_transfers = extract_erc20_transfers(token_transfers)
    
    unified_transfers = internal_eth.unionByName(erc20_transfers, allowMissingColumns=True)
    
    out_path = f"data/stage3_data/unified_transfers/block_start={start_block}/block_end={end_block}/data.parquet"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    unified_transfers.write.mode("overwrite").parquet(out_path)
    
    print(f"Stage 3: {unified_transfers.count()} total transfers extracted to {out_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    args = parser.parse_args()
    run_stage3(args.start_block, args.end_block)
