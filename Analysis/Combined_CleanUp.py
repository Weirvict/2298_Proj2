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
print(f"Beginning of rows: {df.count()}")
#----------------------------------------------------------------------------------------------------------------------------------------------
# Victoria's clean up code
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

# Incase misspelling happens
valid_countries = ["CANADA", "FRANCE", "GERMANY", "GREAT BRITAIN", "MEXICO", "UNITED STATES"]
df_clean_country = df.filter(col("country").isin(valid_countries))

# Calculate Rogue data
math = 15000-df_clean_country.count()
print(f"Rougue Country and City rows: {math}\n")
#------------------------------------------------------------------------------------------------------------------------------------------------
# Kevin's clean up code

# Quantity, product_id clean up
p2_filtered = df.filter(
    col("datetime").isNotNull() &
    col("qty").isNotNull() & (col("qty") > 0) &
    col("product_id").isNotNull()
    )
# Drop duplicates
p2_clean = p2_filtered.dropDuplicates()

# Calculate Rogue data
math2 = (15000 - p2_clean.count()) 
print(f"Rogue quantity and product_id rows: {math2}\n")
#------------------------------------------------------------------------------------------------------------------------------------------------
# Calculate the rogue data
print("Calculations of Rogue data")
print(f"Total %: (({math} + {math2})/15000) * 100 =")
print(((math + math2)/15000)*100)


#--------------------------------------------------------------------------------------------------------------------------------------------------------
# Combine the clean data

# Final column order: 
final_columns = [
    "order_id", "customer_id", "customer_name", "product_id",
    "product_name", "product_category", "payment_type", "qty",
    "price", "datetime", "country", "city",
    "ecommerce_website_name", "payment_txn_id",
    "payment_txn_success", "failure_reason"
]

# Save to CSV file:
output_path = f"{dir_path}/final_combined.csv"

# print("Combined CSV created")
#-------------------------------------------------------------------------------------------------------------------------------------------------
print("End of Session")
# Stop the Spark session
spark.stop()