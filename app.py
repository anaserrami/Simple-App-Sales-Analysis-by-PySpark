from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, max, row_number
from pyspark.sql.types import DoubleType, StringType, DateType
from pyspark.sql.window import Window

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

# Task 7: Calculate total sales by category
sales_by_category = sales_df.groupBy("category").agg(sum("amount").alias("total_sales"))
print("\nTask 7: Total Sales by Category:")
sales_by_category.show()

# Task 8: Calculate total sales by month
sales_by_month = sales_df.withColumn("month", month("date")) \
                         .groupBy("month") \
                         .agg(sum("amount").alias("total_sales"))
print("\nTask 8: Total Sales by Month:")
sales_by_month.show()

# Task 9: Identify the top 5 best-selling products by total sales amount
top_products = sales_df.groupBy("product_id") \
                       .agg(sum("amount").alias("total_sales")) \
                       .orderBy(col("total_sales").desc()) \
                       .limit(5)
print("\nTask 9: Top 5 Best-Selling Products by Total Sales Amount:")
top_products.show()

# Task 10: For each category, find the most sold product by amount
window_spec = Window.partitionBy("category").orderBy(col("total_sales").desc())
top_product_per_category = sales_df.groupBy("category", "product_id") \
                                   .agg(sum("amount").alias("total_sales")) \
                                   .withColumn("rank", row_number().over(window_spec)) \
                                   .filter(col("rank") == 1) \
                                   .drop("rank")
print("\nTask 10: Top Product by Sales in Each Category:")
top_product_per_category.show()

# Stop the Spark session
spark.stop()
