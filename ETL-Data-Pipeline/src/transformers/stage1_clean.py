"""
Stage 1: Clean
This script reads the raw data, fixes data types (like converting hex strings to actual numbers), 
normalizes wallet addresses to lowercase, extracts ERC-20 token transfers from logs, 
and removes duplicate rows to create a clean, standardized dataset.
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DecimalType, ArrayType, StringType
import decimal

def get_spark():
    return SparkSession.builder.appName("stage1_clean").master("local[*]").getOrCreate()

@F.udf(returnType=LongType())
def to_long(val):
    if val is None: return None
    try:
        val_str = str(val).strip()
        if not val_str: return None
        if val_str.startswith("0x"):
            return int(val_str, 16)
        return int(float(val_str))
    except:
        return None

@F.udf(returnType=DecimalType(38, 0))
def to_decimal(val):
    if val is None: return None
    try:
        val_str = str(val).strip()
        if not val_str or val_str == "0x": return None
        if val_str.startswith("0x"):
            hex_str = val_str[:66]
            num = int(hex_str, 16)
        else:
            num = int(float(val_str))
            
        if num >= (10 ** 38):
            return None
            
        return decimal.Decimal(num)
    except:
        return None

def clean_transactions(df):
    return (
        df
        .filter(F.col("hash").isNotNull())
        .withColumn("value", to_decimal("value"))
        .withColumn("gas", to_long("gas"))
        .withColumn("gas_price", to_long("gas_price"))
        .withColumn("block_number", to_long("block_number"))
        .withColumn("transaction_index", to_long("transaction_index"))
        .dropDuplicates(["hash"])
    )

def clean_blocks(df):
    return (
        df
        .filter(F.col("hash").isNotNull())
        .withColumn("number", to_long("number"))
        .withColumn("timestamp", to_long("timestamp"))
        .withColumn("gas_used", to_long("gas_used"))
        .withColumn("gas_limit", to_long("gas_limit"))
        .dropDuplicates(["hash"])
    )

def clean_traces(df):
    return (
        df
        .withColumn("value", F.coalesce(to_decimal("value"), F.lit(0)))
        .withColumn("call_type", F.lower(F.col("call_type")))
        .withColumn("block_number", to_long("block_number"))
        .dropDuplicates(["transaction_hash", "trace_address"])
    )

def clean_logs_to_token_transfers(df):
    df = df.withColumn("topics_arr", F.from_json("topics", ArrayType(StringType())))
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    transfers = df.filter(F.col("topics_arr")[0] == TRANSFER_TOPIC)
    
    def parse_address(col):
        return F.concat(F.lit("0x"), F.substring(col, 27, 40))

    
    transfers = transfers.withColumn("from_address", F.lower(parse_address(F.col("topics_arr")[1])))
    transfers = transfers.withColumn("to_address", F.lower(parse_address(F.col("topics_arr")[2])))
    transfers = transfers.withColumn("value", to_decimal("data"))
    transfers = transfers.withColumn("token_address", F.lower(F.col("address")))
    transfers = transfers.withColumn("block_number", to_long("block_number"))
    transfers = transfers.withColumn("log_index", to_long("log_index"))

    
    return transfers.dropDuplicates(["transaction_hash", "log_index"]).select(
        "transaction_hash", "block_number", "log_index", "token_address", "from_address", "to_address", "value"
    )

def run_stage1(start_block: int, end_block: int):
    spark = get_spark()
    
    data_types = {
        "transactions": clean_transactions,
        "blocks": clean_blocks,
        "traces": clean_traces,
        "logs": clean_logs_to_token_transfers,
    }
    
    for dt, cleaner_fn in data_types.items():
        input_dir = f"data/raw_data/{dt}/block_start={start_block}/block_end={end_block}"
        if not os.path.exists(input_dir) or not os.listdir(input_dir):
            print(f"Skipping {dt}, directory not found or empty: {input_dir}")
            continue
            
        df = spark.read.parquet(input_dir)
        clean_df = cleaner_fn(df)
        
        out_dt = "token_transfers" if dt == "logs" else dt
        output_dir = f"data/stage1_data/{out_dt}/block_start={start_block}/block_end={end_block}"
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "data.parquet")
        
        clean_df.write.mode("overwrite").parquet(out_path)
        print(f"Stage 1 complete for {out_dt}: {clean_df.count()} rows saved to {out_path}")
        
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    args = parser.parse_args()
    run_stage1(args.start_block, args.end_block)
