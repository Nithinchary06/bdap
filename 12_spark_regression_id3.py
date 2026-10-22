# EXPERIMENT 12 — SPARK ML LINEAR REGRESSION AND ID3
# Aim: Predict Boston Housing MEDV using Spark ML linear regression and record ID3 steps.
# Place instructor-supplied housing.csv beside this script. Expected headers are listed below.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("BostonHousing").getOrCreate()

# Load data. MEDV is the target; the remaining columns are model features.
df = spark.read.csv("housing.csv", header=True, inferSchema=True)
features = [
    "CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE",
    "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"
]
assembler = VectorAssembler(inputCols=features, outputCol="features")
data = assembler.transform(df).select("features", col("MEDV").alias("label"))

# Split data reproducibly into training and test subsets.
train, test = data.randomSplit([0.8, 0.2], seed=42)

# Fit the regression model and display predicted vs actual values.
model = LinearRegression(featuresCol="features", labelCol="label").fit(train)
predictions = model.transform(test)
predictions.select("prediction", "label").show(5)

# Evaluate prediction error with root mean squared error.
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse"
)
print("RMSE:", evaluator.evaluate(predictions))
spark.stop()

# ID3 ALGORITHM (record pseudocode; this is not executed by the Spark regression script):
# ID3(data, attributes):
#   if all rows have the same class: return that class
#   if attributes is empty: return majority class
#   choose attribute with highest information gain
#   for each value of chosen attribute:
#       subset = rows having that value
#       create branch using ID3(subset, remaining attributes)
#   return decision tree
# Information Gain = Entropy(parent) - weighted Entropy(children)
#
# EXECUTION: spark-submit 12_spark_regression_id3.py
# Note: Use the housing.csv supplied by your instructor; dataset availability varies.
