from pyspark.sql.functions import col, hour, to_timestamp, countDistinct,col,regexp_replace,when,trim,length
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CleanDataTeam2").getOrCreate()

df = spark.read.csv("file:///mnt/c/Users/xiuzh/2298_Proj2/Data_For_Team2.csv", header=True, inferSchema=True)
sc = spark.sparkContext
sc.setLogLevel("Error")

#--------------------------------------------------------------
print("-----------------------------------------------------------------------")
print("Initial count:", df.count())
print("-----------------------------------------------------------------------")

# 1 === Check if order_id matches exactly one payment_txn_id ===
order_check = (
    df.groupBy("order_id")
      .agg(countDistinct("payment_txn_id").alias("unique_txn_ids"))
      .filter("unique_txn_ids > 1")
)

if order_check.count() > 0:
    print("Bad order_id rows:", order_check.count())
    order_check.show(truncate=False)

before = df.count()
df = df.join(order_check, on="order_id", how="left_anti")
after = df.count()
print(f"Step1 removed: {before - after}, remaining: {after}")
print("-----------------------------------------------------------------------")
# It did not has any problems so that we are not going to do anything about it.

# 2 === Check if product_id maps to exactly one product_name ===
# we used same method here, but found out it doesn't work bc too many data got
# deleted so we decided to groupby and print out data to see the potencial problems.


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
      .show(200, truncate=False)
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
      .show(200, truncate=False)
)
before = df.count()


df = df.filter((col("product_name").isNotNull()) & (length(trim(col("product_name"))) > 0))

df = df.join(multi_name_ids, on="product_id", how="left_anti")
after = df.count()
print(f"Step2 removed: {before - after}, remaining: {after}")
print("-----------------------------------------------------------------------")



# 3 === Clean pattern and value checks ===

cols_to_count = ["product_category","payment_txn_success","qty", "price", "datetime"]

for c in cols_to_count:
    print(f"\n================ VALUE COUNTS FOR {c.upper()} ================")
    df.groupBy(c).count().orderBy(c).show(20, truncate=False)
#There is no weird items in product_category,so just check if is NULL.

# for payment_txn_success, it has only"F"&"Y"

before = df.count()
df_clean = df.filter(
    (col("order_id").isNotNull()) & (col("order_id").rlike("^oid_[1-9][0-9]*$")) &
    (col("product_id").isNotNull()) & (col("product_id").rlike("^pid_[1-9][0-9]*$")) &
    ( col("product_category").isNotNull())&
    (col("qty").isNotNull()) & (col("qty") > 0) &
    (col("price").isNotNull()) & (col("price") > 0) &
    (col("datetime").isNotNull()) &
    (col("payment_txn_success").isin("Y","F"))
)
after = df_clean.count()
print(f"Step3 removed: {before - after}, remaining: {after}")
print("-----------------------------------------------------------------------")

print("Final Clean Count:", df_clean.count())
print("-----------------------------------------------------------------------")


