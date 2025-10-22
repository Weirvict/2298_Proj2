from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("dataframe Example").getOrCreate()
sc = spark.sparkContext

sc.setLogLevel("WARN")

df = spark.read.csv("file:///mnt/c/Users/shash/OneDrive/Desktop/Revature/Week 5/2298_Proj2/Data_For_Team2.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("data")

df_one = df.dropDuplicates()

df_filtered = df_one.filter(
  col("order_id").isNotNull() &
  col("product_category").isNotNull() &
  col("qty").isNotNull() & (col("qty") > 0) &
  col("product_id").isNotNull() & 
  col("price").isNotNull() & (col("price") > 0)
)

print(df_filtered.count())
#df_filtered.show()
query = spark.sql("SELECT DISTINCT(product_category) FROM data")
#ecommerce_website_name has \t
query.show()

spark.stop()