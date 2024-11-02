from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, max, row_number
from pyspark.sql.types import DoubleType, StringType, DateType

# Start a Spark session
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# Load the CSV data
file_path = "/app/sales_data.csv"
sales_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Task 1: Display an overview of the data
print("\nTask 1: Overview of the data:")
sales_df.show()

# Task 2: Check schema and count rows
print("\nTask 2: Schema of the data:")
sales_df.printSchema()
row_count = sales_df.count()
print(f"\nTask 2: Number of rows: {row_count}")

# Task 3: Filter transactions with amount > 100
filtered_sales_df = sales_df.filter(col("amount") > 100)
print("\nTask 3: Transactions with amount > 100:")
filtered_sales_df.show()

# Task 4: Replace null values in 'amount' with 0 and 'category' with 'Unknown'
sales_df = sales_df.fillna({"amount": 0, "category": "Unknown"})
print("\nTask 4: Data after replacing null values:")
sales_df.show()

# Task 5: Convert 'date' column to DateType for time-based analysis
print("\nTask 5: Data before date conversion:")
sales_df.select("date").show(5)  # Show only the first 5 rows for brevity

# Perform the conversion
sales_df = sales_df.withColumn("date", col("date").cast(DateType()))

print("\nTask 5: Data after date conversion:")
sales_df.select("date").show(5)

# Task 6: Calculate total sales for the entire period
total_sales = sales_df.agg(sum("amount").alias("total_sales")).collect()[0]["total_sales"]
print(f"\nTask 6: Total Sales Amount for the entire period: {total_sales}")

# Stop the Spark session
spark.stop()
