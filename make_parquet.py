import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------------------------------------------------
# Argument parsing
# -------------------------------------------------------------------
# Expect: --INPUT_PATH <s3://...> --OUTPUT_PATH <s3://...>
args = sys.argv
try:
    input_path = args[args.index("--INPUT_PATH") + 1]
    output_dir = args[args.index("--OUTPUT_PATH") + 1]
except (ValueError, IndexError):
    print("Usage: spark-submit script.py --INPUT_PATH <s3://...> --OUTPUT_PATH <s3://...>")
    sys.exit(1)

# -------------------------------------------------------------------
# Spark session
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("ChromosomeSplitJob")
    .getOrCreate()
)

# -------------------------------------------------------------------
# Chromosomes list
# -------------------------------------------------------------------
chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY", "chrM"]

# -------------------------------------------------------------------
# Read CSVs from S3
# -------------------------------------------------------------------
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(input_path)
)

# -------------------------------------------------------------------
# Split and write by chromosome
# -------------------------------------------------------------------
for chrom in chromosomes:
    df_chr = df.filter(col("variant_id").startswith(chrom + ":"))
    if df_chr.count() == 0:
        continue

    df_chr.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(os.path.join(output_dir, f"{chrom}/"))

print("Processing completed!")
spark.stop()