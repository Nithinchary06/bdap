# EXPERIMENT 9 — PYSPARK RDD OPERATIONS
# Aim: Count employees, filter joining year, map names, and average salary by department.
# Put employee_data.csv in the same directory as this script.
# Expected CSV header: EmployeeID,Name,Department,Salary,YearOfJoining

from pyspark.sql import SparkSession

# Start a Spark session and get its lower-level RDD context.
spark = SparkSession.builder.appName("EmployeeRDD").getOrCreate()
sc = spark.sparkContext

# Read the CSV lines and remove the header.
lines = sc.textFile("employee_data.csv")
header = lines.first()
rows = lines.filter(lambda line: line != header)

# Convert each CSV row into typed fields.
# This simple split assumes no quoted commas inside fields.
def parse(line):
    p = line.split(",")
    return (p[0], p[1], p[2], float(p[3]), int(p[4]))

emp = rows.map(parse)

# Count records, filter employees who joined after 2014, and extract names.
print("Employee count:", emp.count())
print("Joined after 2014:", emp.filter(lambda x: x[4] > 2014).collect())
print("Employee names:", emp.map(lambda x: x[1]).collect())

# Aggregate (salary sum, employee count) by department, then divide to get average.
dept_sums = emp.map(lambda x: (x[2], (x[3], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
avg_salary = dept_sums.mapValues(lambda x: x[0] / x[1])
print("Average salary by department:", avg_salary.collect())

# Stop Spark cleanly.
spark.stop()

# EXECUTION from the folder containing both files:
# spark-submit 09_pyspark_rdd.py
