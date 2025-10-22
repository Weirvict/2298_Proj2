from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, when

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

df.printSchema()

# Just use as comparison
dfo = df

# Country and City clean up

# Make city and country names uppercase
df = (
    df.withColumn("country", upper(trim(col("country"))))
      .withColumn("city", upper(trim(col("city"))))
)

# Drop rows where city or country are null plus eccomerce website
df = df.dropna(subset=["city", "country", "ecommerce_website_name"])

# Remove tabs, newlines, and extra spaces
df = df.withColumn("city", regexp_replace("city", r"[\t\n\r]+", " "))
df = df.withColumn("city", regexp_replace("city", r"\s+", " "))
df = df.withColumn("city", trim(col("city")))

# Get the countries
countries = df.select("country").distinct().orderBy("country")
countries.show(100, truncate=False)

# Incase misspelling happens
valid_countries = ["CANADA", "FRANCE", "GERMANY", "GREAT BRITAIN", "MEXICO", "UNITED STATES"]
df_clean_country = df.filter(col("country").isin(valid_countries))
# print("Original rows: ", dfo.count())
# print("Rows before:", df.count())
# print("Rows after:", df_clean_country.count())

# for c in valid_countries:
#     print(f"\n--- {c} ---")
#     df_clean_country.filter(col("country") == c) \
#         .select("city") \
#         .distinct() \
#         .orderBy("city") \
#         .show(100, truncate=False)

df.select("ecommerce_website_name").distinct().show(truncate=False)

df_cleaned = df.withColumn(
    "ecommerce_website_name",
    trim(col("ecommerce_website_name"))  # remove leading/trailing spaces
)

# remove literal '\t' characters that appear as text or tabs
df_cleaned = df_cleaned.withColumn(
    "ecommerce_website_name",
    when(col("ecommerce_website_name").isNull(), None)
    .otherwise(trim(regexp_replace(col("ecommerce_website_name"), r"\\t|\t", "")))
)
df_cleaned.select("ecommerce_website_name").distinct().show()



# Stop the Spark session
spark.stop()