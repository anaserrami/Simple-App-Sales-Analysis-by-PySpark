from pyspark.sql import SparkSession

# Start a Spark session
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# Load the CSV data
file_path = "/app/sales_data.csv"
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Task 1: Display an overview of the data
print("\nTask 1: Overview of the data:")
sales_df.show()

# Stop the Spark session
spark.stop()
