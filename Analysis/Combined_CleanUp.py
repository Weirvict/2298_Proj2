from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, countDistinct, length, when

# Create spark session
spark = SparkSession.builder \
    .appName("Ecommerce Data Cleanup") \
    .getOrCreate()

# Get the Spark context from the Spark session
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Define directory path
dir_path = "file:///mnt/c/Users/Victoria Weir/Revature/Training//proj2/2298_Proj2/analysis"

# Load CSV files into DataFrames
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(f"{dir_path}/Data_For_Team2.csv")
#----------------------------------------------------------------------------------------------------------------------------------------------
# Victoria's clean up code
# Country and City clean up
# Make city and country names uppercase
df = (
    df.withColumn("country", upper(trim(col("country"))))
      .withColumn("city", upper(trim(col("city"))))
)

# Drop rows where city or country are null and website
df = df.dropna(subset=["city", "country", "ecommerce_website_name"])

# Remove tabs, newlines, and extra spaces
df = df.withColumn("city", regexp_replace("city", r"[\t\n\r]+", " "))
df = df.withColumn("city", regexp_replace("city", r"\s+", " "))
df = df.withColumn("city", trim(col("city")))

# Incase misspelling happens
valid_countries = ["CANADA", "FRANCE", "GERMANY", "GREAT BRITAIN", "MEXICO", "UNITED STATES"]
df_clean_country = df.filter(col("country").isin(valid_countries))

# Clean up Ecommerce website
df = df.withColumn(
    "ecommerce_website_name",
    trim(col("ecommerce_website_name"))  # remove leading/trailing spaces
)

# remove literal '\t' characters that appear as text or tabs
df = df.withColumn(
    "ecommerce_website_name",
    when(col("ecommerce_website_name").isNull(), None)
    .otherwise(trim(regexp_replace(col("ecommerce_website_name"), r"\\t|\t", "")))
)

#------------------------------------------------------------------------------------------------------------------------------------------------
# Kevin's clean up code

# Quantity, product_id clean up
df = df.filter(
    col("datetime").isNotNull() &
    col("qty").isNotNull() & (col("qty") > 0) &
    col("product_id").isNotNull()
)

# Drop duplicates
df = df.dropDuplicates()
#------------------------------------------------------------------------------------------------------------------------------------------------
# Xiuzhen's clean up code
# 1 === Check if order_id matches exactly one payment_txn_id ===
order_check = (
    df.groupBy("order_id")
      .agg(countDistinct("payment_txn_id").alias("unique_txn_ids"))
      .filter("unique_txn_ids > 1")
)

df = df.join(order_check, on="order_id", how="left_anti")

# 2 === Check if product_id maps to exactly one product_name ===
multi_name_ids = (
    df.groupBy("product_id")
      .agg(countDistinct("product_name").alias("unique_names"))
      .filter(col("unique_names") > 1)
)

(
    df.join(multi_name_ids, on="product_id", how="inner")
      .groupBy("product_id", "product_name")
      .count()
      .orderBy("product_id", "count", ascending=False)
)

df = (
    df.withColumn(
        "product_name",
        # Step 1. Replace tabs, newlines, and multiple spaces with a single space
        regexp_replace(col("product_name"), r"[\t\n\r]+", " ")
    )
    .withColumn(
        "product_name",
        regexp_replace(col("product_name"), r"\s{2,}", " ")  # collapse multiple spaces
    )
    .withColumn(
        "product_name",
        trim(col("product_name"))  # trim leading and trailing spaces
    )
    .withColumn(
        # Step 2. Replace null, empty string, or literal 'NULL' with None
        "product_name",
        when(
            (col("product_name").isNull()) |
            (col("product_name") == "") |
            (col("product_name").rlike("(?i)^null$")),
            None
        ).otherwise(col("product_name"))
    )
)
multi_name_ids = (
    df.groupBy("product_id")
      .agg(countDistinct("product_name").alias("unique_names"))
      .filter(col("unique_names") > 1)
)

(
    df.join(multi_name_ids, on="product_id", how="inner")
      .groupBy("product_id", "product_name")
      .count()
      .orderBy("product_id", "count", ascending=False)
)

df = df.filter((col("product_name").isNotNull()) & (length(trim(col("product_name"))) > 0))

df = df.join(multi_name_ids, on="product_id", how="left_anti")

# 3 === Clean pattern and value checks ===

cols_to_count = ["product_category","payment_txn_success","qty", "price", "datetime"]

# for payment_txn_success, it has only"F"&"Y"
df = df.withColumn(
    "payment_txn_success",
    regexp_replace(col("payment_txn_success"), "^F$", "N")
)

before = df.count()
df_clean = df.filter(
    (col("order_id").isNotNull()) & (col("order_id").rlike("^oid_[1-9][0-9]*$")) &
    (col("product_id").isNotNull()) & (col("product_id").rlike("^pid_[1-9][0-9]*$")) &
    (col("product_category").isNotNull())&
    (col("qty").isNotNull()) & (col("qty") > 0) &
    (col("price").isNotNull()) & (col("price") > 0) &
    (col("datetime").isNotNull()) &
    (col("payment_txn_success").isin("Y","N"))
)

#--------------------------------------------------------------------------------------------------------------------------------------------------------
# Combine the clean data

# Define final schema order
df_final = df_clean.select(
    "order_id", "customer_id", "customer_name", "product_id",
    "product_name", "product_category", "payment_type", "qty",
    "price", "datetime", "country", "city",
    "ecommerce_website_name", "payment_txn_id",
    "payment_txn_success", "failure_reason"
)

# Save to CSV file:
output_path = f"{dir_path}/clean_data_final"

df_final.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

# Stop the Spark session
spark.stop()