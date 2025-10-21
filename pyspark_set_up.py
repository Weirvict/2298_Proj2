from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("df").getOrCreate()
sc = spark.sparkContext

sc.setLogLevel("WARN")

proj2_df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("file:///mnt/c/Users/kevin/.vscode/Python_Examples/Python_activity/Data_For_Team2.csv",)
)

#print(proj2_df.schema['datetime'].dataType)

print(proj2_df.count())

p2_filtered = proj2_df.filter(
    col("datetime").isNotNull() &
    col("qty").isNotNull() & (col("qty") > 0) &
    col("product_id").isNotNull()
    )


print(p2_filtered.count())

p2_clean = p2_filtered.dropDuplicates()

print(p2_clean.count())

print(p2_clean)

#proj2_df.show()

#p2_clean.describe().show()

#print(p2_clean.groupBy("datetime").count().orderBy("count", ascending=True).show(15000, truncate = False))

print(p2_clean.groupBy("qty").count().orderBy("count", ascending=True).show(15000, truncate = False))

print(p2_clean.groupBy("product_id").count().orderBy("count", ascending=True).show(15000, truncate = False))




spark.stop()