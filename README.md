

## 🧬 AWS EMR Job: Split Variants by Chromosome

**Script name:** `make_parquet.py`
**Purpose:** Reads a variant CSV from S3, splits the data into per-chromosome Parquet files (1–22, X, Y, M), and writes them back to S3.

---

### 📂 1. Upload Script to S3

```bash
aws s3 cp make_parquet.py s3://aws-batch-input-bioinformatics/scripts/make_parquet.py
```

**Example S3 layout:**

```
s3://aws-batch-input-bioinformatics/
│
├── scripts/
│   └── make_parquet.py
│
├── input/
│   └── variants.csv                ← input file
│
└── output_split/                   ← output folder for Parquet files
```

---

### 🔐 2. Create IAM Roles for EMR

Use defaults or create custom:

| Role                  | Type          | Required Policy                    |
| --------------------- | ------------- | ---------------------------------- |
| `EMR_DefaultRole`     | Cluster       | `AmazonElasticMapReduceFullAccess` |
| `EMR_EC2_DefaultRole` | EC2 instances | `AmazonS3FullAccess`               |

To create default ones if not yet created:

```bash
aws emr create-default-roles
```

---

### 🚀 3. Launch an EMR Cluster

```bash
aws emr create-cluster \
--name "chromosome-split-cluster" \
--release-label emr-6.15.0 \
--applications Name=Spark \
--ec2-attributes KeyName=my-key \
--instance-type m5.xlarge \
--instance-count 3 \
--use-default-roles
```

**Parameters**

* `KeyName`: your EC2 key pair for optional SSH
* `instance-type`: compute size
* `instance-count`: total nodes (1 master + 2 core)

Wait for the cluster status to become **WAITING**.

---

### ⚡ 4. Submit the Spark Step

Once the cluster is ready, run:

```bash
aws emr add-steps \
--cluster-id j-XXXXXXXXXXXXX \
--steps Type=Spark,Name="ChromosomeSplitJob",ActionOnFailure=CONTINUE,\
Args=[s3://aws-batch-input-bioinformatics/scripts/make_parquet.py,\
--INPUT_PATH,s3://aws-batch-input-bioinformatics/input/variants.csv,\
--OUTPUT_PATH,s3://aws-batch-input-bioinformatics/output_split/]
```

---

### 🧠 5. Script Explanation

```python
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------------------------------------------------
# Parse arguments
# -------------------------------------------------------------------
args = sys.argv
try:
    input_path = args[args.index("--INPUT_PATH") + 1]
    output_dir = args[args.index("--OUTPUT_PATH") + 1]
except (ValueError, IndexError):
    print("Usage: spark-submit script.py --INPUT_PATH <s3://...> --OUTPUT_PATH <s3://...>")
    sys.exit(1)

# -------------------------------------------------------------------
# Create Spark session
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("ChromosomeSplitJob")
    .getOrCreate()
)

# -------------------------------------------------------------------
# Chromosome list
# -------------------------------------------------------------------
chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY", "chrM"]

# -------------------------------------------------------------------
# Read the CSV from S3
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
```

**Functionality:**

* Reads your input CSV file (with a column like `"variant_id"`, e.g. `chr1:12345:A:T`)
* Filters rows per chromosome
* Writes each filtered dataset as a **Snappy-compressed Parquet file** to:

  ```
  s3://aws-batch-input-bioinformatics/output_split/chr1/
  s3://aws-batch-input-bioinformatics/output_split/chr2/
  ...
  s3://aws-batch-input-bioinformatics/output_split/chrM/
  ```

---

### ⚙️ 6. Example Output on S3

```
s3://aws-batch-input-bioinformatics/output_split/
│
├── chr1/
│   └── part-0000.parquet
├── chr2/
│   └── part-0000.parquet
...
└── chrM/
    └── part-0000.parquet
```

---

### 💰 7. Recommended EMR Setup (for 250 MB CSV)

| Parameter     | Value                                 |
| ------------- | ------------------------------------- |
| Cluster Size  | 1 Master + 2 Core                     |
| Instance Type | m5.xlarge                             |
| Auto-scaling  | Not needed (small dataset)            |
| Runtime       | < 5 minutes                           |
| Cost          | ~$0.10–0.15 total (on-demand pricing) |

---

### ✅ Summary

| Step | Action                                       |
| ---- | -------------------------------------------- |
| 1    | Upload `make_parquet.py` to S3        |
| 2    | Set up EMR roles                             |
| 3    | Launch EMR cluster with Spark                |
| 4    | Submit Spark step job                        |
| 5    | Get output Parquet files split by chromosome |

---

