from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace

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

# Country and City clean up

# Make city and country names uppercase
df = (
    df.withColumn("country", upper(trim(col("country"))))
      .withColumn("city", upper(trim(col("city"))))
)

# Drop rows where city or country are null
df = df.dropna(subset=["city", "country"])

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
print("Rows before:", df.count())
print("Rows after:", df_clean_country.count())

for c in valid_countries:
    print(f"\n--- {c} ---")
    df_clean_country.filter(col("country") == c) \
        .select("city") \
        .distinct() \
        .orderBy("city") \
        .show(100, truncate=False)

# Price clean up
df.select("price").show(10)




# Stop the Spark session
spark.stop()