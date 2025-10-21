from pyspark.sql.functions import col, hour, to_timestamp,sum,countDistinct
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CleanDataTeam2").getOrCreate()

df = spark.read.csv("Data_For_Team2.csv", header=True, inferSchema=True)


#--------------------------------------------------------------


#1.	order_id match payment_txn_id otherwise we are not sure if data is valid.
#2.	need to make sure product_id match product_name,
#3. clean

#-------------------------------------------------------------------------

# 1 === Check if order_id matches exactly one payment_txn_id ===
order_check = (
    df.groupBy("order_id")
      .agg(countDistinct("payment_txn_id").alias("unique_txn_ids"))
      .filter("unique_txn_ids > 1")
)

if order_check.count > 0:
    order_check.show(truncate=False)  # shows which order_ids are problematic

# Remove bad orders
df = df.join(order_check, on="order_id", how="left_anti")

# 2 === Check if product_id maps to exactly one product_name ===
product_check = (
    df.groupBy("product_id")
      .agg(countDistinct("product_name").alias("unique_names"))
      .filter("unique_names > 1")
)

product_mismatch_count = product_check.count()


if product_check.count() > 0:
    product_check.show(truncate=False)

df = df.join(product_check, on="product_id", how="left_anti")

#Removes those rows from the original DataFrame (anti-join = keep everything not in that mismatch list)

#3.clean 

df_clean = df.filter(
    # Order ID pattern: must start with oid_ and end with positive digits
    (col("order_id").isNotNull()) &
    (col("order_id").rlike("^oid_[1-9][0-9]*$")) &

    # Product ID pattern: must start with pid_ and end with positive digits
    (col("product_id").isNotNull()) &
    (col("product_id").rlike("^pid_[1-9][0-9]*$")) &

    #  Product category must be alphabetic
    (col("product_category").isNotNull()) &
    (col("product_category").rlike("^[A-Za-z\\s]+$")) &

    #  Quantity and price must be positive
    (col("qty").isNotNull()) & (col("qty") > 0) &
    (col("price").isNotNull()) & (col("price") > 0) &

    # Datetime valid
    (col("datetime").isNotNull()) &

    #  Country and city alphabetic only
    (col("country").isNotNull()) & (col("country").rlike("^[A-Za-z\\s]+$")) &
    (col("city").isNotNull()) & (col("city").rlike("^[A-Za-z\\s]+$")) &

    #  Payment success flag
    (col("payment_txn_success").isin("Y", "N"))
)
#---------------------------------------------------
#-----------------What times have the highest traffic of sales? Per country?---------------#

#Highest traffic of sales means 
# 1.highest revenue
# 2.highest quantity sale 
# 3.more transaction order? 
#### does it included failure one? Bc placed an order but failure also cause traffic? 


df = df.select("order_id","datetime","country","qty","price","payment_txn_success")

df = df.withColumn("datetime", to_timestamp("datetime"))
df = df.withColumn("hour",hour("datetime"))

df.show(5)
spark.stop()
