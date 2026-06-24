from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
for dt in ["blocks", "transactions", "traces", "logs"]:
    try:
        df = spark.read.parquet(f"data/raw_data/{dt}/")
        print(f"--- {dt} ---")
        df.printSchema()
    except Exception as e:
        print(e)
spark.stop()
