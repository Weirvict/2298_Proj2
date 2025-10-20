from pyspark.sql import SparkSession

# Create spark session
spark = SparkSession.builder.appName("Ecommerce Data Cleanup").getOrCreate()

# Get the Spark context from the Spark session
sc = spark.sparkContext

# Load in csv file
df = spark.read.csv("Data_For_Team2.csv", header=True)



# Stop the Spark session
spark.stop()