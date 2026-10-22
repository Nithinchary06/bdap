# EXPERIMENT 10 — PYSPARK DATAFRAME ANALYSIS AND NULL CLEANING
# Aim: Compute department averages, salary percentage difference, years of service,
# salary categories, and show a JDBC null-cleaning pattern.
# Put employee_data.csv beside this script. Run with spark-submit.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, year, current_date, round, when
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Load the supplied CSV and infer numeric data types.
df = spark.read.csv("employee_data.csv", header=True, inferSchema=True)
df.show()

# Calculate average salary per department.
df.groupBy("Department").agg(
    round(avg("Salary"), 2).alias("AvgSalary")
).show()

# Add department average and percentage difference from that average.
w = Window.partitionBy("Department")
df = df.withColumn("DeptAvg", avg("Salary").over(w))
df = df.withColumn(
    "SalaryPercentageDifference",
    round((col("Salary") - col("DeptAvg")) / col("DeptAvg") * 100, 2)
)

# Calculate approximate years since joining, based on the current calendar year.
df = df.withColumn(
    "YearsWithCompany", year(current_date()) - col("YearOfJoining")
)

# Example salary bands: use thresholds from your own record if they differ.
df = df.withColumn(
    "SalaryCategory",
    when(col("Salary") < 50000, "Low")
    .when(col("Salary") <= 80000, "Medium")
    .otherwise("High")
)
df.show(truncate=False)
spark.stop()

# DATABASE NULL-CLEANING PATTERN (optional; configure JDBC URL, driver and credentials first).
# from pyspark.sql.functions import col, avg
# jdbc_url = "jdbc:your_database://host:port/database"
# props = {"user": "YOUR_USER", "password": "YOUR_PASSWORD",
#          "driver": "YOUR_JDBC_DRIVER_CLASS"}
# dbdf = spark.read.jdbc(url=jdbc_url, table="employees", properties=props)
# dbdf.printSchema()
# dbdf.select([col(c).isNull().alias(c) for c in dbdf.columns]).show()
# mean_age = dbdf.select(avg("age")).first()[0]
# cleaned = dbdf.fillna({"age": mean_age, "name": "Unknown"})
# cleaned.write.jdbc(url=jdbc_url, table="employees_cleaned",
#                    mode="overwrite", properties=props)
