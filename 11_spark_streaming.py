# EXPERIMENT 11 — SPARK STREAMING WORD COUNT
# Aim: Read live text from a TCP socket and show streaming word counts.
# Run a socket server in one terminal and this script in another terminal.

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("StreamingWC").getOrCreate()

# Read lines arriving at localhost:9999 as a streaming DataFrame.
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999).load()

# Split each line into words and create one row per word.
words = lines.select(explode(split(lines.value, " ")).alias("word"))
counts = words.groupBy("word").count()

# Print the full running count table to the console.
query = counts.writeStream.outputMode("complete") \
    .format("console").start()
query.awaitTermination()

# EXECUTION:
# Terminal 1: nc -lk 9999
# Terminal 2: spark-submit 11_spark_streaming.py
# Type: hello spark hello   (in terminal 1)
# Stop both processes with Ctrl+C. Requires compatible Spark/Java and netcat.
